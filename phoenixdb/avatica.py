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
import httplib
import pprint
import json
import math
import logging
import urlparse
import time
from decimal import Decimal
from HTMLParser import HTMLParser
from phoenixdb import errors

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
    match = re.match(r'^(?:([^ ]+): )?ERROR (\d+) \(([0-9A-Z]{5})\): (.*?)$', message)
    if match is not None:
        exception, code, sqlstate, message = match.groups()
        raise_sql_error(int(code), sqlstate, message)


def parse_error_page(html):
    parser = JettyErrorPageParser()
    parser.feed(html)
    if parser.title == ['HTTP ERROR: 500']:
        message = ' '.join(parser.message).strip()
        parse_and_raise_sql_error(message)
        raise errors.InternalError(message)


def parse_error_json(text):
    data = json.loads(text)
    if data.get('response') == 'error':
        message = data.get('errorMessage', '')
        parse_and_raise_sql_error(message)
        code = data.get('errorCode', -1)
        sqlstate = data.get('sqlState', '00000')
        raise_sql_error(code, sqlstate, message)
        raise errors.InternalError(message)


AVATICA_1_2_0 = (1, 2, 0)
AVATICA_1_3_0 = (1, 3, 0)
AVATICA_1_4_0 = (1, 4, 0)
AVATICA_1_5_0 = (1, 5, 0)
AVATICA_1_6_0 = (1, 6, 0)


class AvaticaClient(object):
    """Client for Avatica's RPC server.

    This exposes all low-level functionality that the Avatica
    server provides, using the native terminology. You most likely
    do not want to use this class directly, but rather get connect
    to a server using :func:`phoenixdb.connect`.
    """

    def __init__(self, url, version=None, max_retries=None):
        """Constructs a new client object.
        
        :param url:
            URL of an Avatica RPC server.
        :param version:
            Version of the Avarica RPC server.
        """
        self.url = parse_url(url)
        if version is not None:
            self.version = version
        else:
            self.version = AVATICA_1_3_0
            query = urlparse.parse_qs(self.url.query)
            for v in query.get('v', []):
                if v in ('1.2.0', '1.2'):
                    self.version = AVATICA_1_2_0
                elif v in ('1.3.0', '1.3'):
                    self.version = AVATICA_1_3_0
                elif v in ('1.4.0', '1.4'):
                    self.version = AVATICA_1_4_0
                elif v in ('1.5.0', '1.5'):
                    self.version = AVATICA_1_5_0
                elif v in ('1.6.0', '1.6'):
                    self.version = AVATICA_1_6_0
                else:
                    raise errors.ProgrammingError('Unknown Avatica version')
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

        class FakeFloat(float):
            # XXX there has to be a better way to do this
            def __init__(self, value):
                self.value = value
            def __repr__(self):
                return str(self.value)

        def default(obj):
            if isinstance(obj, Decimal):
                return FakeFloat(obj)
            raise TypeError

        if self.version >= AVATICA_1_4_0:
            body = json.dumps(request_data, default=default)
            headers = {'content-type': 'application/json'}
        else:
            body = None
            headers = {'request': json.dumps(request_data, default=default)}

        response = self._post_request(body, headers)
        response_body = response.read()

        if response.status != httplib.OK:
            logger.debug("Received response\n%s", response_body)
            if '<html>' in response_body:
                parse_error_page(response_body)
            if response.getheader('content-type', '').startswith('application/json'):
                parse_error_json(response_body)
            raise errors.InterfaceError('RPC request returned invalid status code', response.status)

        noop = lambda x: x
        try:
            response_data = json.loads(response_body, parse_float=noop)
        except ValueError as e:
            logger.debug("Received response\n%s", response_body)
            raise errors.InterfaceError('valid JSON document', cause=e)

        logger.debug("Received response\n%s", pprint.pformat(response_data))

        if 'response' not in response_data:
            raise errors.InterfaceError('missing response type')

        if expected_response_type is None:
            expected_response_type = request_data['request']

        if response_data['response'] != expected_response_type:
            raise errors.InterfaceError('unexpected response type "{}"'.format(response_data['response']))

        return response_data

    def getCatalogs(self):
        request = {'request': 'getCatalogs'}
        return self._apply(request)

    def getSchemas(self, catalog=None, schemaPattern=None):
        request = {
            'request': 'getSchemas',
            'catalog': catalog,
            'schemaPattern': schemaPattern,
        }
        return self._apply(request)

    def getTables(self, catalog=None, schemaPattern=None, tableNamePattern=None, typeList=None):
        request = {
            'request': 'getTables',
            'catalog': catalog,
            'schemaPattern': schemaPattern,
            'tableNamePattern': tableNamePattern,
            'typeList': typeList,
        }
        return self._apply(request)

    def getColumns(self, catalog=None, schemaPattern=None, tableNamePattern=None, columnNamePattern=None):
        request = {
            'request': 'getColumns',
            'catalog': catalog,
            'schemaPattern': schemaPattern,
            'tableNamePattern': tableNamePattern,
            'columnNamePattern': columnNamePattern,
        }
        return self._apply(request)

    def getTableTypes(self):
        request = {'request': 'getTableTypes'}
        return self._apply(request)

    def getTypeInfo(self):
        request = {'request': 'getTypeInfo'}
        return self._apply(request)

    def connectionSync(self, connectionId, connProps=None):
        """Synchronizes connection properties with the server.

        :param connectionId:
            ID of the current connection.

        :param connProps:
            Dictionary with the properties that should be changed.

        :returns:
            Dictionary with the current properties.
        """
        request = {
            'request': 'connectionSync',
            'connectionId': connectionId,
            'connProps': {
                'connProps': 'connPropsImpl',
                'autoCommit': None,
                'readOnly': None,
                'transactionIsolation': None,
                'catalog': None,
                'schema': None,
                'dirty': None,
            },
        }
        if connProps is not None:
            request['connProps'].update(connProps)
        return self._apply(request)['connProps']

    def openConnection(self, connectionId, info=None):
        """Opens a new connection.

        New in avatica 1.5.

        :param connectionId:
            ID of the connection to close.
        """
        if self.version < AVATICA_1_5_0:
            return
        request = {
            'request': 'openConnection',
            'connectionId': connectionId,
        }
        if info is not None:
            request['info'] = info
        self._apply(request)

    def closeConnection(self, connectionId):
        """Closes a connection.

        :param connectionId:
            ID of the connection to close.
        """
        request = {
            'request': 'closeConnection',
            'connectionId': connectionId,
        }
        self._apply(request)

    def createStatement(self, connectionId):
        """Creates a new statement.

        :param connectionId:
            ID of the current connection.

        :returns:
            New statement ID.
        """
        request = {
            'request': 'createStatement',
            'connectionId': connectionId,
        }
        return self._apply(request)['statementId']

    def closeStatement(self, connectionId, statementId):
        """Closes a statement.

        :param connectionId:
            ID of the current connection.

        :param statementId:
            ID of the statement to close.
        """
        request = {
            'request': 'closeStatement',
            'connectionId': connectionId,
            'statementId': statementId,
        }
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
        request = {
            'request': 'prepareAndExecute',
            'connectionId': connectionId,
            'sql': sql,
            'maxRowCount': maxRowCount,
        }
        if self.version >= AVATICA_1_4_0:
            request['statementId'] = statementId
        if self.version >= AVATICA_1_5_0:
            response_type = 'executeResults'
        else:
            response_type = 'Service$ExecuteResponse'
        return self._apply(request, response_type)['results']

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
        request = {
            'request': 'prepare',
            'connectionId': connectionId,
            'sql': sql,
            'maxRowCount': maxRowCount,
        }
        return self._apply(request)['statement']

    def execute(self, connectionId, statementId, parameterValues=None, maxRowCount=-1):
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

        :param maxRowCount:
            Maximum number of rows to return; negative means no limit.

        :returns:
            Frame data, or ``None`` if there are no more.
        """
        request = {
            'request': 'execute',
            'statementHandle': {
                'connectionId': connectionId,
                'id': statementId,
            },
            'parameterValues': parameterValues,
            'maxRowCount': maxRowCount,
        }
        return self._apply(request, 'executeResults')['results']

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
        request = {
            'request': 'fetch',
            'connectionId': connectionId,
            'statementId': statementId,
            'offset': offset,
            'fetchMaxRowCount': fetchMaxRowCount,
        }
        if self.version < AVATICA_1_3_0:
            # XXX won't work for all types, but oh well...
            request['parameterValues'] = [v['value'] for v in parameterValues]
        elif self.version < AVATICA_1_5_0:
            request['parameterValues'] = parameterValues
        else:
            raise errors.InternalError('fetch with parameterValues not supported by avatica 1.5+')
        return self._apply(request)['frame']

    def supportsExecute(self):
        return self.version >= AVATICA_1_5_0

