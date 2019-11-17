

class DriverError(Exception):
    def __init__(self, *args, **kwargs):
        self._kw_args = kwargs
        self._args = args

## bugs

class InternalDriverError(DriverError):
    pass

class StartupTimeout(InternalDriverError):
    pass

class RequestTimeout(InternalDriverError):
    pass


## user errors
class UsageException(DriverError):
    pass

class MaximumStreamsException(UsageException):
    pass

class TypeViolation(UsageException):
    pass


## server errors
class ServerError(DriverError):
    pass

class VersionMismatchException(ServerError):
    pass

class ProtocolException(ServerError):
    """
    Data violates the contracts in the specification
    """
class UnknownPayloadException(ServerError):
    """
    Data doesn't follow the datagrams specification
    """
