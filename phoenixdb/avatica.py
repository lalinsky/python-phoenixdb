"""Implementation of the JSON-over-HTTP RPC protocol used by Avatica."""

import httplib
import pprint
import json
import logging
import urlparse
from HTMLParser import HTMLParser
from phoenixdb.errors import OperationalError, InternalError

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
        top_tag = self.path.pop()
        if tag != top_tag:
            raise Exception('mismatched tags')

    def handle_data(self, data):
        if len(self.path) > 2 and self.path[0] == 'html' and self.path[1] == 'body':
            if len(self.path) == 3 and self.path[2] == 'h2':
                self.title.append(data.strip())
            elif len(self.path) == 4 and self.path[2] == 'p' and self.path[3] == 'pre':
                self.message.append(data.strip())


class AvaticaClient(object):
    """Client for Avatica's RPC server.

    This exposes all low-level functionality that the Avatica
    server provides, using the native terminology. You most likely
    do not want to use this class directly, but rather get connect
    to a server using :func:`phoenixdb.connect`.
    """

    def __init__(self, url):
        """Constructs a new client object.
        
        :param url:
            URL of an Avatica RPC server.
        """
        self.url = urlparse.urlparse(url)
        self.connection = None

    def connect(self):
        """Opens a HTTP connection to the RPC server."""
        logger.debug("Opening connection to %s:%s", self.url.hostname, self.url.port)
        try:
            self.connection = httplib.HTTPConnection(self.url.hostname, self.url.port)
            self.connection.connect()
        except httplib.HTTPException as e:
            raise OperationalError('Unable to connect to the specified service', e)

    def close(self):
        """Closes the HTTP connection to the RPC server."""
        if self.connection is not None:
            logger.debug("Closing connection to %s:%s", self.url.hostname, self.url.port)
            try:
                self.connection.close()
            except httplib.HTTPException as e:
                logger.warning("Error while closing connection", exc_info=True)
            self.connection = None

    def _parse_error_page(self, body):
        parser = JettyErrorPageParser()
        parser.feed(body)
        if parser.title == ['HTTP ERROR: 500']:
            raise OperationalError(' '.join(parser.message))

    def _apply(self, request_data, expected_response_type=None):
        logger.debug("Sending request\n%s", pprint.pformat(request_data))
        try:
            self.connection.request('POST', self.url.path, headers={'request': json.dumps(request_data)})
            response = self.connection.getresponse()
        except httplib.HTTPException as e:
            raise OperationalError('RPC request failed', e)

        response_body = response.read()

        if response.status != httplib.OK:
            logger.debug("Received response\n%s", response_body)
            if '<html>' in response_body:
                self._parse_error_page(response_body)
            raise OperationalError('RPC request returned invalid status code', response.status)

        try:
            response_data = json.loads(response_body)
        except ValueError as e:
            logger.debug("Received response\n%s", response_body)
            raise InternalError('valid JSON document', e)

        logger.debug("Received response\n%s", pprint.pformat(response_data))

        if 'response' not in response_data:
            raise InternalError('missing response type')

        if expected_response_type is None:
            expected_response_type = request_data['request']

        if response_data['response'] != expected_response_type:
            raise InternalError('unexpected response type', response_data['response'])

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
#            'statementId': statement_id,
            'sql': sql,
            'maxRowCount': maxRowCount,
        }
        return self._apply(request, 'Service$ExecuteResponse')['results']

    def prepare(self, connectionId, statementId, sql, maxRowCount=-1):
        """Prepares a statement.

        :param connectionId:
            ID of the current connection.

        :param statementId:
            ID of the statement to prepare.

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
#            'statementId': statementId,
            'sql': sql,
            'maxRowCount': maxRowCount,
        }
        return self._apply(request)['statement']

    def fetch(self, connectionId, statementId, parameterValues, offset=0, fetchMaxRowCount=-1):
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
            'parameterValues': parameterValues,
            'offset': offset,
            'fetchMaxRowCount': fetchMaxRowCount,
        }
        return self._apply(request)['frame']

