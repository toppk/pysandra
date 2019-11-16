

class DriverError(Exception):
    pass

class InternalDriverError(DriverError):
    def __init__(self, *args, **kwargs):
        self._kw_args = kwargs
        self._args = args

class MaximumStreamsException(DriverError):
    pass

class ServerError(DriverError):
    pass
