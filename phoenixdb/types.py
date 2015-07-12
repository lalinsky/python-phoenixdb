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

__all__ = [
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary', 'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID', 'BOOLEAN',
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
