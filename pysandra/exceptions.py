from typing import Any


class DriverError(Exception):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)
        self._display_keys = list(kwargs.keys())
        self._args = args

    def __str__(self) -> str:
        args = []
        if len(self._args) > 0:
            args.append(("description", f"{self._args[0]!r}"))
        for key in self._display_keys:
            args.append((key, f"{self.__dict__[key]!r}"))
        text = ", ".join([f"{k}={v}" for k, v in args])
        return f"{self.__class__.__name__}({text})"


# bugs


class InternalDriverError(DriverError):
    pass


class StartupTimeout(InternalDriverError):
    pass


class RequestTimeout(InternalDriverError):
    pass


# user errors
class UsageException(DriverError):
    pass


class MaximumStreamsException(UsageException):
    pass


class TypeViolation(UsageException):
    pass


class BadInputException(UsageException):
    pass


# server errors
class ServerError(DriverError):
    pass


class ConnectionDroppedError(ServerError):
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
