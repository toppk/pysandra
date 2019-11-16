

class DriverError(Exception):
    pass

## bugs

class InternalDriverError(DriverError):
    def __init__(self, *args, **kwargs):
        self._kw_args = kwargs
        self._args = args


## user errors
class UsageException(DriverError):
    def __init__(self, *args, **kwargs):
        self._kw_args = kwargs
        self._args = args

class MaximumStreamsException(UsageException):
    pass

class TypeViolation(UsageException):
    pass



## server errors
class ServerError(DriverError):
    def __init__(self, *args, **kwargs):
        self._kw_args = kwargs
        self._args = args

class VersionMismatchException(DriverError):
    pass
