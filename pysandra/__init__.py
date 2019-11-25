from .__about__ import __description__, __title__, __version__
from .client import Client
from .connection import Connection
from .constants import Consistency, Events
from .exceptions import (  # noqa: F401
    BadInputException,
    InternalDriverError,
    ServerError,
)
from .types import Rows, SchemaChange  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)

__all__ = [
    "__description__",
    "__title__",
    "__version__",
    "Client",
    "Consistency",
    "Events",
    "Connection",
    "Rows",
    "SchemaChange",
    "BadInputException",
    "InternalDriverError",
    "ServerError",
]


logger.warning(
    "***WARNING*** This software is in an early development state.  "
    "There will be bugs and major features missing.  "
    "Please see http://github.com/toppk/pysandra for details!!!"
)
