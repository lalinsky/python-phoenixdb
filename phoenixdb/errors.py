__all__ = [
    'Warning', 'Error', 'InterfaceError', 'DatabaseError', 'DataError',
    'OperationalError', 'IntegrityError', 'InternalError',
    'ProgrammingError', 'NotSupportedError',
]


class Warning(StandardError):
    pass


class Error(StandardError):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
