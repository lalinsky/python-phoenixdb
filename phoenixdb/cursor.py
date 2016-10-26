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

import logging
import collections
import base64
import datetime
from decimal import Decimal
# TODO what are the other types used for, other than tests?
from phoenixdb.types import Binary
from phoenixdb.errors import OperationalError, NotSupportedError, ProgrammingError, InternalError
# TODO conditionally import based on the connection string
from phoenixdb.schema.calcite1_8 import requests_pb2, common_pb2, responses_pb2

__all__ = ['Cursor', 'ColumnDescription']

logger = logging.getLogger(__name__)

# TODO this can probably be replaced
ColumnDescription = collections.namedtuple('ColumnDescription', 'name type_code display_size internal_size precision scale null_ok')
"""Named tuple for representing results from :attr:`Cursor.description`."""


def time_from_java_sql_time(n):
    dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=n)
    return dt.time()


def time_to_java_sql_time(t):
    return ((t.hour * 60 + t.minute) * 60 + t.second) * 1000 + t.microsecond / 1000


def date_from_java_sql_date(n):
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=n)


def date_to_java_sql_date(d):
    if isinstance(d, datetime.datetime):
        d = d.date()
    td = d - datetime.date(1970, 1, 1)
    return td.days


def datetime_from_java_sql_timestamp(n):
    return datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=n)


def datetime_to_java_sql_timestamp(d):
    td = d - datetime.datetime(1970, 1, 1)
    return td.microseconds / 1000 + (td.seconds + td.days * 24 * 3600) * 1000


class Cursor(object):
    """Database cursor for executing queries and iterating over results.

    You should not construct this object manually, use :meth:`Connection.cursor() <phoenixdb.connection.Connection.cursor>` instead.
    """

    arraysize = 1
    """
    Read/write attribute specifying the number of rows to fetch
    at a time with :meth:`fetchmany`. It defaults to 1 meaning to
    fetch a single row at a time.
    """

    itersize = 2000
    """
    Read/write attribute specifying the number of rows to fetch
    from the backend at each network roundtrip during iteration
    on the cursor. The default is 2000.
    """

    def __init__(self, connection, id=None):
        self._connection = connection
        self._id = id
        self._signature = None
        self._column_data_types = []
        self._frame = None
        self._pos = None
        self._closed = False
        self.arraysize = self.__class__.arraysize
        self.itersize = self.__class__.itersize
        self._updatecount = -1

    def __del__(self):
        if not self._connection._closed and not self._closed:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not self._closed:
            self.close()

    def __iter__(self):
        return self

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self):
        """Closes the cursor.
        No further operations are allowed once the cursor is closed.

        If the cursor is used in a ``with`` statement, this method will
        be automatically called at the end of the ``with`` block.
        """
        if self._closed:
            raise ProgrammingError('the cursor is already closed')
        if self._id is not None:
            self._connection._client.closeStatement(self._connection._id, self._id)
            self._id = None
        self._signature = None
        self._column_data_types = []
        self._frame = None
        self._pos = None
        self._closed = True

    @property
    def closed(self):
        """Read-only attribute specifying if the cursor is closed or not."""
        return self._closed

    @property
    def description(self):
        if self._signature is None:
            return None
        description = []
        for column in self._signature['columns']:
            # TODO
            description.append(ColumnDescription(
                column['columnName'],
                column['type']['name'],
                column['displaySize'],
                None,
                column['precision'],
                column['scale'],
                bool(column['nullable']),
            ))
        return description

    def _set_id(self, id):
        if self._id is not None and self._id != id:
            self._connection._client.closeStatement(self._connection._id, self._id)
        self._id = id

    def _convert_class(self, class_name):
        """Converts a given Java class name.

        :param class_name:
            The JDBC class name.

        :returns: tuple ``(rep, field_name, cast_type, mutate_type)``
            WHERE
            ``rep`` is a ``common_pb2.Rep`` type
            ``field_name`` is the field name in ``common_pb2.TypedValue``
            ``cast_type`` is the method to cast the JDBC value to the Python value
            ``mutate_type`` is the method to mutate the Python value to the JDBC value
        """
        rep = None
        field_name = None
        cast_type = None
        mutate_type = None

        # TODO move this to a separate class?
        if class_name == 'java.lang.String':
            rep = common_pb2.STRING
            field_name = 'string_value'
            cast_type = str
        elif class_name == 'java.math.BigDecimal':
            rep = common_pb2.BIG_DECIMAL
            field_name = 'string_value'
            cast_type = Decimal
        elif class_name == 'java.lang.Boolean':
            rep = common_pb2.BOOLEAN
            field_name = 'boolean_value'
            cast_type = bool
        elif class_name == 'java.lang.Byte':
            rep = common_pb2.BYTE
            field_name = 'number_value'
            cast_type = bytes
        elif class_name == 'java.lang.Short':
            rep = common_pb2.SHORT
            field_name = 'number_value'
            cast_type = int
        elif class_name == 'java.lang.Integer':
            rep = common_pb2.INTEGER
            field_name = 'number_value'
            cast_type = int
        elif class_name == 'java.lang.Long':
            rep = common_pb2.LONG
            field_name = 'number_value'
            cast_type = int
        elif class_name == 'java.lang.Float':
            rep = common_pb2.FLOAT
            field_name = 'double_value'
            cast_type = float
        elif class_name == 'java.lang.Double':
            rep = common_pb2.DOUBLE
            field_name = 'double_value'
            cast_type = float
        elif class_name == '[B':
            rep = common_pb2.BYTE_STRING
            field_name = 'bytes_value'
            cast_type = base64.b64decode
            mutate_type = Binary
        elif class_name == 'java.sql.Date':
            rep = common_pb2.JAVA_SQL_DATE
            field_name = 'number_value'
            cast_type = date_from_java_sql_date
            mutate_type = date_to_java_sql_date
        elif class_name == 'java.sql.Time':
            rep = common_pb2.JAVA_SQL_TIME
            field_name = 'number_value'
            cast_type = time_from_java_sql_time
            mutate_type = time_to_java_sql_time
        elif class_name == 'java.sql.Timestamp':
            rep = common_pb2.JAVA_SQL_TIMESTAMP
            field_name = 'number_value'
            cast_type = datetime_from_java_sql_timestamp
            mutate_type = datetime_to_java_sql_timestamp
        # TODO should this be org.apache.phoenix.schema.types.PhoenixArray
        # TODO arrays and structs are handled differently
        #elif class_name == 'java.lang.Array':
        #    rep = common_pb2.ARRAY
        #elif class_name == 'java.lang.Struct':
        #    rep = common_pb2.STRUCT
        else:
            rep = common_pb2.OBJECT

        return rep, field_name, cast_type, mutate_type

    def _set_signature(self, signature):
        self._signature = signature
        self._column_data_types = []
        self._parameter_data_types = []
        if signature is None:
            return
        identity = lambda value: value

        for i, column in enumerate(signature.columns):
            dtype = self._convert_class(column.column_class_name)
            self._column_data_types.append(dtype)

        for parameter in signature.parameters:
            print 'GETTING PARAM FOR SIG', parameter
            dtype = self._convert_class(parameter.class_name)
            self._parameter_data_types.append(dtype)

    def _transform_parameters(self, parameters):
        typed_parameters = []
        for value, data_type in zip(parameters, self._parameter_data_types):
            typed_value = common_pb2.TypedValue()

            if value is None:
                typed_value.null = True
                typed_value.type = common_pb2.NULL
            else:
                typed_value.null = False

                # use the mutator function
                if data_type[3] is not None:
                    value = data_type[3](value)

                # set the Rep field
                typed_value.type = data_type[0]

                # set the field_name to the value
                setattr(typed_value, data_type[1], value)
            typed_parameters.append(typed_value)
        return typed_parameters

    def _set_frame(self, frame):
        self._frame = frame
        self._pos = None

        if frame is not None:
            if frame.rows:
                self._pos = 0
            # TODO although 'rows' is empty, 'done' seems to always be False...why?
            #elif not frame.done:
            #    raise InternalError('got an empty frame, but the statement is not done yet')

    def _fetch_next_frame(self):
        offset = self._frame.offset + len(self._frame.rows)
        frame = self._connection._client.fetch(self._connection._id, self._id,
            offset=offset, fetchMaxRowCount=self.itersize)
        self._set_frame(frame)

    def _process_results(self, results):
        if results:
            result = results[0]
            if result.own_statement:
                self._set_id(result.statement_id)
            self._set_signature(result.signature)
            self._set_frame(result.first_frame)
            self._updatecount = result.update_count

    def execute(self, operation, parameters=None):
        if self._closed:
            raise ProgrammingError('the cursor is already closed')
        self._updatecount = -1
        self._set_frame(None)
        if parameters is None:
            if self._id is None:
                self._set_id(self._connection._client.createStatement(self._connection._id))
            results = self._connection._client.prepareAndExecute(self._connection._id, self._id,
                operation, maxRowCount=self.itersize)
            self._process_results(results)
        else:
            statement = self._connection._client.prepare(self._connection._id,
                operation, maxRowCount=self.itersize)
            self._set_id(statement.id)
            self._set_signature(statement.signature)

            results = self._connection._client.execute(self._connection._id, self._id,
                statement.signature, self._transform_parameters(parameters),
                maxRowCount=self.itersize)
            self._process_results(results)

    def executemany(self, operation, seq_of_parameters):
        if self._closed:
            raise ProgrammingError('the cursor is already closed')
        self._updatecount = -1
        self._set_frame(None)
        statement = self._connection._client.prepare(self._connection._id,
            operation, maxRowCount=0)
        self._set_id(statement['id'])
        self._set_signature(statement['signature'])
        for parameters in seq_of_parameters:
            self._connection._client.execute(self._connection._id, self._id,
                self._transform_parameters(parameters),
                maxRowCount=0)

    def fetchone(self):
        if self._frame is None:
            raise ProgrammingError('no select statement was executed')
        if self._pos is None:
            return None
        rows = self._frame.rows
        row = self._transform_row(rows[self._pos])
        self._pos += 1
        if self._pos >= len(rows):
            self._pos = None
            if not self._frame.done:
                self._fetch_next_frame()
        return row

    def _transform_row(self, row):
        """Transforms a Row into Python values.

        :param row:
            A ``common_pb2.Row`` object.

        :returns:
            A list of values casted into the correct Python types.
        """
        tmp = []

        for i, column in enumerate(row.value):
            # TODO handle arrays, structs
            if column.has_array_value:
                pass
            elif column.scalar_value.null:
                tmp.append(None)
            else:
                dtype = self._column_data_types[i]

                # get the value from the field_name
                value = getattr(column.scalar_value, dtype[1])

                # cast the value
                # TODO try/catch the casting
                value = dtype[2](value)

                tmp.append(value)
        return tmp

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        rows = []
        while size > 0:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
            size -= 1
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    @property
    def connection(self):
        """Read-only attribute providing access to the :class:`Connection <phoenixdb.connection.Connection>` object this cursor was created from."""
        return self._connection


    @property
    def rowcount(self):
        """Read-only attribute specifying the number of rows affected by
        the last executed DML statement or -1 if the number cannot be
        determined. Note that this will always be set to -1 for select
        queries."""
        return self._updatecount

    @property
    def rownumber(self):
        """Read-only attribute providing the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined.

        The index can be seen as index of the cursor in a sequence
        (the result set). The next fetch operation will fetch the
        row indexed by :attr:`rownumber` in that sequence.
        """
        if self._frame is not None and self._pos is not None:
            return self._frame['offset'] + self._pos
        return self._pos
