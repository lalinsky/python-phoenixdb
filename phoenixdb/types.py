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

import time
import datetime
import base64
from decimal import Decimal
from phoenixdb.calcite import common_pb2

__all__ = [
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary', 'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID', 'BOOLEAN',
    'REP_TYPES', 'REP_TYPES_MAP', 'PHOENIX_PARAMETERS', 'PHOENIX_PARAMETERS_MAP',
    'TypeHelper',
]


def Date(year, month, day):
    """Constructs an object holding a date value."""
    return datetime.date(year, month, day)


def Time(hour, minute, second):
    """Constructs an object holding a time value."""
    return datetime.time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    """Constructs an object holding a datetime/timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    """Constructs an object holding a date value from the given UNIX timestamp."""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Constructs an object holding a time value from the given UNIX timestamp."""
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Constructs an object holding a datetime/timestamp value from the given UNIX timestamp."""
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(value):
    """Constructs an object capable of holding a binary (long) string value."""
    if isinstance(value, _BinaryString):
        return value
    return _BinaryString(base64.b64encode(value))


class _BinaryString(str):
    pass


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


class ColumnType(object):

    def __init__(self, eq_types):
        self.eq_types = tuple(eq_types)
        self.eq_types_set = set(eq_types)

    def __cmp__(self, other):
        if other in self.eq_types_set:
            return 0
        if other < self.eq_types:
            return 1
        else:
            return -1


STRING = ColumnType(['VARCHAR', 'CHAR'])
"""Type object that can be used to describe string-based columns."""

BINARY = ColumnType(['BINARY', 'VARBINARY'])
"""Type object that can be used to describe (long) binary columns."""

NUMBER = ColumnType(['INTEGER', 'UNSIGNED_INT', 'BIGINT', 'UNSIGNED_LONG', 'TINYINT', 'UNSIGNED_TINYINT', 'SMALLINT', 'UNSIGNED_SMALLINT', 'FLOAT', 'UNSIGNED_FLOAT', 'DOUBLE', 'UNSIGNED_DOUBLE', 'DECIMAL'])
"""Type object that can be used to describe numeric columns."""

DATETIME = ColumnType(['TIME', 'DATE', 'TIMESTAMP', 'UNSIGNED_TIME', 'UNSIGNED_DATE', 'UNSIGNED_TIMESTAMP'])
"""Type object that can be used to describe date/time columns."""

ROWID = ColumnType([])
"""Only implemented for DB API 2.0 compatibility, not used."""

BOOLEAN = ColumnType(['BOOLEAN'])
"""Type object that can be used to describe boolean columns. This is a phoenixdb-specific extension."""

# XXX ARRAY


REP_TYPES = {
    'bool_value': [
        common_pb2.PRIMITIVE_BOOLEAN,
        common_pb2.BOOLEAN,
    ],
    'string_value': [
        common_pb2.PRIMITIVE_CHAR,
        common_pb2.CHARACTER,
        common_pb2.STRING,
        common_pb2.BIG_DECIMAL,
        # TODO DECIMAL type (java.math.BigDecimal) is set to a Rep.OBJECT...why?
        common_pb2.OBJECT,
    ],
    'number_value': [
        common_pb2.PRIMITIVE_BYTE,
        common_pb2.PRIMITIVE_INT,
        common_pb2.PRIMITIVE_SHORT,
        common_pb2.PRIMITIVE_LONG,
        common_pb2.BYTE,
        common_pb2.INTEGER,
        common_pb2.SHORT,
        common_pb2.LONG,
        common_pb2.BIG_INTEGER,
        common_pb2.JAVA_SQL_TIME,
        common_pb2.JAVA_SQL_TIMESTAMP,
        common_pb2.JAVA_SQL_DATE,
        common_pb2.JAVA_UTIL_DATE,
        common_pb2.NUMBER,
    ],
    'bytes_value': [
        common_pb2.BYTE_STRING,
    ],
    'double_value': [
        common_pb2.PRIMITIVE_FLOAT,
        common_pb2.PRIMITIVE_DOUBLE,
        common_pb2.FLOAT,
        common_pb2.DOUBLE,
    ]
}
"""Groups of Rep types."""

REP_TYPES_MAP = dict( (v, k) for k in REP_TYPES for v in REP_TYPES[k] )
"""Flips the available types to allow for faster lookup by Rep."""


PHOENIX_PARAMETERS = {
    'bool_value': [
        ('BOOLEAN', common_pb2.BOOLEAN),
    ],
    'string_value': [
        ('CHAR', common_pb2.CHARACTER),
        ('VARCHAR', common_pb2.STRING),
        ('DECIMAL', common_pb2.BIG_DECIMAL),
    ],
    'number_value': [
        ('INTEGER', common_pb2.INTEGER),
        ('UNSIGNED_INT', common_pb2.INTEGER),
        ('BIGINT', common_pb2.LONG),
        ('UNSIGNED_LONG', common_pb2.LONG),
        ('TINYINT', common_pb2.BYTE),
        ('UNSIGNED_TINYINT', common_pb2.BYTE),
        ('SMALLINT', common_pb2.SHORT),
        ('UNSIGNED_SMALLINT', common_pb2.SHORT),
        ('DATE', common_pb2.JAVA_SQL_DATE),
        ('UNSIGNED_DATE', common_pb2.JAVA_SQL_DATE),
        ('TIME', common_pb2.JAVA_SQL_TIME),
        ('UNSIGNED_TIME', common_pb2.JAVA_SQL_TIME),
        ('TIMESTAMP', common_pb2.JAVA_SQL_TIMESTAMP),
        ('UNSIGNED_TIMESTAMP', common_pb2.JAVA_SQL_TIMESTAMP),
    ],
    'bytes_value': [
        ('BINARY', common_pb2.BYTE_STRING),
        ('VARBINARY', common_pb2.BYTE_STRING),
    ],
    'double_value': [
        ('FLOAT', common_pb2.FLOAT),
        ('UNSIGNED_FLOAT', common_pb2.FLOAT),
        ('DOUBLE', common_pb2.DOUBLE),
        ('UNSIGNED_DOUBLE', common_pb2.DOUBLE),
    ]
}
"""Map Phoenix types to Reps."""

PHOENIX_PARAMETERS_MAP = dict( (v[0], (k, v[1])) for k in PHOENIX_PARAMETERS for v in PHOENIX_PARAMETERS[k] )
"""Flips the Phoenix types to allow for faster lookup by type."""


class TypeHelper(object):
    @staticmethod
    def from_rep(rep):
        """Finds a method to cast from a Python value into a Phoenix type.

        :param rep:
            The Rep enum from ``common_pb2.Rep``.
=
        :returns: tuple ``(field_name, cast_type)``
            WHERE
            ``field_name`` is the attribute in ``common_pb2.TypedValue``
            ``cast_type`` is the method to cast from the Phoenix value to the Python value
        """
        field_name = None
        cast_type = None

        if rep in REP_TYPES_MAP:
            field_name = REP_TYPES_MAP[rep]

        if  rep == common_pb2.BIG_DECIMAL:
            cast_type = Decimal
        elif rep == common_pb2.BYTE:
            cast_type = bytes
        elif rep == common_pb2.BYTE_STRING:
            cast_type = base64.b64decode
        elif rep == common_pb2.JAVA_SQL_DATE:
            cast_type = date_from_java_sql_date
        elif rep == common_pb2.JAVA_SQL_TIME:
            cast_type = time_from_java_sql_time
        elif rep == common_pb2.JAVA_SQL_TIMESTAMP:
            cast_type = datetime_from_java_sql_timestamp

        return field_name, cast_type

    @staticmethod
    def from_phoenix(type_name):
        """Converts a Phoenix parameter type into a Rep and a mutator method.

        :param type_name:
            The JDBC type name from ``common_pb2.AvaticaParameter``.
=
        :returns: tuple ``(field_name, rep, mutate_type)``
            WHERE
            ``field_name`` is the attribute in ``common_pb2.TypedValue``
            ``rep`` is a ``common_pb2.Rep`` type
            ``mutate_type`` is the method to mutate the Python value to the JDBC value
        """
        field_name = None
        rep = None
        mutate_type = None

        if type_name in PHOENIX_PARAMETERS_MAP:
            field_name, rep = PHOENIX_PARAMETERS_MAP[type_name]

        if  type_name == 'DECIMAL':
            mutate_type = str
        elif type_name == 'BINARY' or type_name == 'VARBINARY':
            mutate_type = Binary
        elif type_name == 'DATE':
            mutate_type = date_to_java_sql_date
        elif type_name == 'TIME':
            mutate_type = time_to_java_sql_time
        elif type_name == 'TIMESTAMP':
            mutate_type = datetime_to_java_sql_timestamp

        return field_name, rep, mutate_type
