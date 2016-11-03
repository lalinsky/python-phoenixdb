# Copyright 2015 Lukas Lalinsky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implementation of the JSON-over-HTTP RPC protocol used by Avatica."""

import re
import socket
import pprint
import math
import logging
import time
from phoenixdb import errors
from phoenixdb.calcite import requests_pb2, common_pb2, responses_pb2

try:
    import httplib
except ImportError:
    import http.client as httplib

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

try:
    from HTMLParser import HTMLParser
except ImportError:
    from html.parser import HTMLParser

__all__ = ['AvaticaClient']

logger = logging.getLogger(__name__)


class JettyErrorPageParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.path = []
        self.title = []
        self.message = []

    def handle_starttag(self, tag, attrs):
        self.path.append(tag)

    def handle_endtag(self, tag):
        self.path.pop()

    def handle_data(self, data):
        if len(self.path) > 2 and self.path[0] == 'html' and self.path[1] == 'body':
            if len(self.path) == 3 and self.path[2] == 'h2':
                self.title.append(data.strip())
            elif len(self.path) == 4 and self.path[2] == 'p' and self.path[3] == 'pre':
                self.message.append(data.strip())


def parse_url(url):
    url = urlparse.urlparse(url)
    if not url.scheme and not url.netloc and url.path:
        netloc = url.path
        if ':' not in netloc:
            netloc = '{}:8765'.format(netloc)
        return urlparse.ParseResult('http', netloc, '/', '', '', '')
    return url


# Defined in phoenix-core/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java
SQLSTATE_ERROR_CLASSES = [
    ('08', errors.OperationalError), # Connection Exception
    ('22018', errors.IntegrityError), # Constraint violatioin.
    ('22', errors.DataError), # Data Exception
    ('23', errors.IntegrityError), # Constraint Violation
    ('24', errors.InternalError), # Invalid Cursor State
    ('25', errors.InternalError), # Invalid Transaction State
    ('42', errors.ProgrammingError), # Syntax Error or Access Rule Violation
    ('XLC', errors.OperationalError), # Execution exceptions
    ('INT', errors.InternalError), # Phoenix internal error
]


def raise_sql_error(code, sqlstate, message):
    for prefix, error_class in SQLSTATE_ERROR_CLASSES:
        if sqlstate.startswith(prefix):
            raise error_class(message, code, sqlstate)


def parse_and_raise_sql_error(message):
    match = re.findall(r'(?:([^ ]+): )?ERROR (\d+) \(([0-9A-Z]{5})\): (.*?) ->', message)
    if match is not None and len(match):
        exception, code, sqlstate, message = match[0]
        raise_sql_error(int(code), sqlstate, message)


def parse_error_page(html):
    parser = JettyErrorPageParser()
    parser.feed(html)
    if parser.title == ['HTTP ERROR: 500']:
        message = ' '.join(parser.message).strip()
        parse_and_raise_sql_error(message)
        raise errors.InternalError(message)


def parse_error_protobuf(text):
    message = common_pb2.WireMessage()
    message.ParseFromString(text)

    err = responses_pb2.ErrorResponse()
    err.ParseFromString(message.wrapped_message)

    parse_and_raise_sql_error(err.error_message)
    raise_sql_error(err.error_code, err.sql_state, err.error_message)
    raise errors.InternalError(err.error_message)


class AvaticaClient(object):
    """Client for Avatica's RPC server.

    This exposes all low-level functionality that the Avatica
    server provides, using the native terminology. You most likely
    do not want to use this class directly, but rather get connect
    to a server using :func:`phoenixdb.connect`.
    """

    def __init__(self, url, max_retries=None):
        """Constructs a new client object.

        :param url:
            URL of an Avatica RPC server.
        """
        self.url = parse_url(url)
        self.max_retries = max_retries if max_retries is not None else 3
        self.connection = None

    def connect(self):
        """Opens a HTTP connection to the RPC server."""
        logger.debug("Opening connection to %s:%s", self.url.hostname, self.url.port)
        try:
            self.connection = httplib.HTTPConnection(self.url.hostname, self.url.port)
            self.connection.connect()
        except (httplib.HTTPException, socket.error) as e:
            raise errors.InterfaceError('Unable to connect to the specified service', e)

    def close(self):
        """Closes the HTTP connection to the RPC server."""
        if self.connection is not None:
            logger.debug("Closing connection to %s:%s", self.url.hostname, self.url.port)
            try:
                self.connection.close()
            except httplib.HTTPException as e:
                logger.warning("Error while closing connection", exc_info=True)
            self.connection = None

    def _post_request(self, body, headers):
        retry_count = self.max_retries
        while True:
            logger.debug("POST %s %r %r", self.url.path, body, headers)
            try:
                self.connection.request('POST', self.url.path, body=body, headers=headers)
                response = self.connection.getresponse()
            except httplib.HTTPException as e:
                if retry_count > 0:
                    delay = math.exp(-retry_count)
                    logger.debug("HTTP protocol error, will retry in %s seconds...", delay, exc_info=True)
                    self.close()
                    self.connect()
                    time.sleep(delay)
                    retry_count -= 1
                    continue
                raise errors.InterfaceError('RPC request failed', cause=e)
            else:
                if response.status == httplib.SERVICE_UNAVAILABLE:
                    if retry_count > 0:
                        delay = math.exp(-retry_count)
                        logger.debug("Service unavailable, will retry in %s seconds...", delay, exc_info=True)
                        time.sleep(delay)
                        retry_count -= 1
                        continue
                return response

    def _apply(self, request_data, expected_response_type=None):
        logger.debug("Sending request\n%s", pprint.pformat(request_data))

        request_name = request_data.__class__.__name__
        message = common_pb2.WireMessage()
        message.name = 'org.apache.calcite.avatica.proto.Requests${}'.format(request_name)
        message.wrapped_message = request_data.SerializeToString()
        body = message.SerializeToString()
        headers = {'content-type': 'application/x-google-protobuf'}

        response = self._post_request(body, headers)
        response_body = response.read()

        if response.status != httplib.OK:
            logger.debug("Received response\n%s", response_body)
            if b'<html>' in response_body:
                parse_error_page(response_body)
            else:
                # assume the response is in protobuf format
                parse_error_protobuf(response_body)
            raise errors.InterfaceError('RPC request returned invalid status code', response.status)

        message = common_pb2.WireMessage()
        message.ParseFromString(response_body)

        logger.debug("Received response\n%s", message)

        if expected_response_type is None:
            expected_response_type = request_name.replace('Request', 'Response')

        expected_response_type = 'org.apache.calcite.avatica.proto.Responses$' + expected_response_type
        if message.name != expected_response_type:
            raise errors.InterfaceError('unexpected response type "{}"'.format(message.name))

        return message.wrapped_message

    def getCatalogs(self, connectionId):
        request = requests_pb2.CatalogsRequest()
        request.connection_id = connectionId
        return self._apply(request)

    def getSchemas(self, connectionId, catalog=None, schemaPattern=None):
        request = requests_pb2.SchemasRequest()
        request.connection_id = connectionId
        if catalog is not None:
            request.catalog = catalog
        if schemaPattern is not None:
            request.schema_pattern = schemaPattern
        return self._apply(request)

    def getTables(self, connectionId, catalog=None, schemaPattern=None, tableNamePattern=None, typeList=None):
        request = requests_pb2.TablesRequest()
        request.connection_id = connectionId
        if catalog is not None:
            request.catalog = catalog
        if schemaPattern is not None:
            request.schema_pattern = schemaPattern
        if tableNamePattern is not None:
            request.table_name_pattern = tableNamePattern
        if typeList is not None:
            request.type_list = typeList
        if typeList is not None:
            request.type_list.extend(typeList)
        request.has_type_list = typeList is not None
        return self._apply(request)

    def getColumns(self, connectionId, catalog=None, schemaPattern=None, tableNamePattern=None, columnNamePattern=None):
        request = requests_pb2.ColumnsRequest()
        request.connection_id = connectionId
        if catalog is not None:
            request.catalog = catalog
        if schemaPattern is not None:
            request.schema_pattern = schemaPattern
        if tableNamePattern is not None:
            request.table_name_pattern = tableNamePattern
        if columnNamePattern is not None:
            request.column_name_pattern = columnNamePattern
        return self._apply(request)

    def getTableTypes(self, connectionId):
        request = requests_pb2.TableTypesRequest()
        request.connection_id = connectionId
        return self._apply(request)

    def getTypeInfo(self, connectionId):
        request = requests_pb2.TypeInfoRequest()
        request.connection_id = connectionId
        return self._apply(request)

    def connectionSync(self, connectionId, connProps=None):
        """Synchronizes connection properties with the server.

        :param connectionId:
            ID of the current connection.

        :param connProps:
            Dictionary with the properties that should be changed.

        :returns:
            A ``common_pb2.ConnectionProperties`` object.
        """
        if connProps is None:
            connProps = {}

        request = requests_pb2.ConnectionSyncRequest()
        request.connection_id = connectionId
        request.conn_props.auto_commit = connProps.get('autoCommit', False)
        request.conn_props.has_auto_commit = True
        request.conn_props.read_only = connProps.get('readOnly', False)
        request.conn_props.has_read_only = True
        request.conn_props.transaction_isolation = connProps.get('transactionIsolation', 0)
        request.conn_props.catalog = connProps.get('catalog', '')
        request.conn_props.schema = connProps.get('schema', '')

        response_data = self._apply(request)
        response = responses_pb2.ConnectionSyncResponse()
        response.ParseFromString(response_data)
        return response.conn_props

    def openConnection(self, connectionId, info=None):
        """Opens a new connection.

        :param connectionId:
            ID of the connection to open.
        """
        request = requests_pb2.OpenConnectionRequest()
        request.connection_id = connectionId
        if info is not None:
            request.info = info

        response_data = self._apply(request)
        response = responses_pb2.OpenConnectionResponse()
        response.ParseFromString(response_data)

    def closeConnection(self, connectionId):
        """Closes a connection.

        :param connectionId:
            ID of the connection to close.
        """
        request = requests_pb2.CloseConnectionRequest()
        request.connection_id = connectionId
        self._apply(request)

    def createStatement(self, connectionId):
        """Creates a new statement.

        :param connectionId:
            ID of the current connection.

        :returns:
            New statement ID.
        """
        request = requests_pb2.CreateStatementRequest()
        request.connection_id = connectionId

        response_data = self._apply(request)
        response = responses_pb2.CreateStatementResponse()
        response.ParseFromString(response_data)
        return response.statement_id

    def closeStatement(self, connectionId, statementId):
        """Closes a statement.

        :param connectionId:
            ID of the current connection.

        :param statementId:
            ID of the statement to close.
        """
        request = requests_pb2.CloseStatementRequest()
        request.connection_id = connectionId
        request.statement_id = statementId

        self._apply(request)

    def prepareAndExecute(self, connectionId, statementId, sql, maxRowCount=-1):
        """Prepares and immediately executes a statement.

        :param connectionId:
            ID of the current connection.

        :param statementId:
            ID of the statement to prepare.

        :param sql:
            SQL query.

        :param maxRowCount:
            Maximum number of rows to return; negative means no limit.

        :returns:
            Result set with the signature of the prepared statement and the first frame data.
        """
        request = requests_pb2.PrepareAndExecuteRequest()
        request.connection_id = connectionId
        request.sql = sql
        request.max_row_count = maxRowCount
        request.max_rows_total = -1
        request.statement_id = statementId

        response_data = self._apply(request, 'ExecuteResponse')
        response = responses_pb2.ExecuteResponse()
        response.ParseFromString(response_data)
        logger.info('results %r', response.results)
        return response.results

    def prepare(self, connectionId, sql, maxRowCount=-1):
        """Prepares a statement.

        :param connectionId:
            ID of the current connection.

        :param sql:
            SQL query.

        :param maxRowCount:
            Maximum number of rows to return; negative means no limit.

        :returns:
            Signature of the prepared statement.
        """
        request = requests_pb2.PrepareRequest()
        request.connection_id = connectionId
        request.sql = sql
        request.max_row_count = maxRowCount
        request.max_rows_total = -1

        response_data = self._apply(request)
        response = responses_pb2.PrepareResponse()
        response.ParseFromString(response_data)
        return response.statement

    def execute(self, connectionId, statementId, signature, parameterValues=None, maxRowCount=-1):
        """Returns a frame of rows.

        The frame describes whether there may be another frame. If there is not
        another frame, the current iteration is done when we have finished the
        rows in the this frame.

        :param connectionId:
            ID of the current connection.

        :param statementId:
            ID of the statement to fetch rows from.

        :param signature:
            common_pb2.Signature object

        :param parameterValues:
            A list of parameter values, if statement is to be executed; otherwise ``None``.

        :param maxRowCount:
            Maximum number of rows to return; negative means no limit.

        :returns:
            Frame data, or ``None`` if there are no more.
        """
        request = requests_pb2.ExecuteRequest()
        request.statementHandle.id = statementId
        request.statementHandle.connection_id = connectionId
        if parameterValues is not None:
            request.parameter_values.extend(parameterValues)
        request.has_parameter_values = parameterValues is not None
        request.statementHandle.signature.CopyFrom(signature)
        # TODO ExecuteRequest has no max_row_count

        response_data = self._apply(request)
        response = responses_pb2.ExecuteResponse()
        response.ParseFromString(response_data)
        return response.results

    def fetch(self, connectionId, statementId, parameterValues=None, offset=0, fetchMaxRowCount=-1):
        """Returns a frame of rows.

        The frame describes whether there may be another frame. If there is not
        another frame, the current iteration is done when we have finished the
        rows in the this frame.

        :param connectionId:
            ID of the current connection.

        :param statementId:
            ID of the statement to fetch rows from.

        :param parameterValues:
            A list of parameter values, if statement is to be executed; otherwise ``None``.

        :param offset:
            Zero-based offset of first row in the requested frame.

        :param fetchMaxRowCount:
            Maximum number of rows to return; negative means no limit.

        :returns:
            Frame data, or ``None`` if there are no more.
        """
        request = requests_pb2.FetchRequest()
        request.connection_id = connectionId
        request.statement_id = statementId
        request.offset = offset
        request.fetch_max_row_count = fetchMaxRowCount

        response_data = self._apply(request)
        response = responses_pb2.FetchResponse()
        response.ParseFromString(response_data)
        return response.frame
