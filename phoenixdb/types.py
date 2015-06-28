import time
import datetime

__all__ = [
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'Binary', 'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',
]


def Date(year, month, day):
    """Constructs an object holding a date value."""
    return datetime.date(year, month, day).isoformat()


def Time(hour, minute, second):
    """Constructs an object holding a time value."""
    return datetime.time(hour, minute, second).isoformat()


def Timestamp(year, month, day, hour, minute, second):
    """Constructs an object holding a datetime/timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second).isoformat()


def DateFromTicks(ticks):
    """Constructs an object holding a date value from the given UNIX timestamp."""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Constructs an object holding a time value from the given UNIX timestamp."""
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Constructs an object holding a datetime/timestamp value from the given UNIX timestamp."""
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(string):
    """Constructs an object capable of holding a binary (long) string value."""
    return string


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

# XXX BOOLEAN, ARRAY
