from typing import Callable, Optional, Tuple

from .codecs import NETWORK_ORDER, STypes, get_struct
from .constants import SERVER_SENT
from .exceptions import InternalDriverError, VersionMismatchException
from .messages import (
    ExecuteMessage,
    OptionsMessage,
    PrepareMessage,
    QueryMessage,
    RegisterMessage,
    RequestMessage,
    StartupMessage,
)
from .types import ExpectedType  # noqa: F401
from .types import ExpectedResponses
from .utils import get_logger

logger = get_logger(__name__)


# Header = namedtuple('Header', 'version flags stream_id opcode')


class Protocol:
    version: int
    compress: Optional[Callable]

    def __init__(self, server_role: bool = False) -> None:
        self.server_role = server_role

    def decode_header(self, header: bytes) -> Tuple[int, int, int, int, int]:
        version, flags, stream, opcode, length = get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).unpack(header)
        logger.debug(
            f"got head={header!r} containing version={version:x} flags={flags:x} stream={stream:x} opcode={opcode:x} length={length:x}"
        )
        self._check_version(version)
        return version, flags, stream, opcode, length

    def _check_version(self, version: int) -> None:
        if self.server_role:
            expected_version = ~SERVER_SENT & self.version
        else:
            expected_version = SERVER_SENT | self.version
        if version != expected_version:
            raise VersionMismatchException(
                f"received incorrect version from server, got version={hex(version)} expected version={hex(expected_version)}"
            )

    def event_handler(
        self,
        version: int,
        flags: int,
        stream_id: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> None:
        raise InternalDriverError("subclass should implement method")

    def build_response(
        self,
        request: "RequestMessage",
        version: int,
        flags: int,
        stream: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> "ExpectedResponses":
        raise InternalDriverError("subclass should implement method")

    def startup(self, stream_id: int, params: dict) -> "StartupMessage":
        raise InternalDriverError("subclass should implement method")

    def query(self, stream_id: int, params: dict) -> "QueryMessage":
        raise InternalDriverError("subclass should implement method")

    def execute(self, stream_id: int, params: dict) -> "ExecuteMessage":
        raise InternalDriverError("subclass should implement method")

    def prepare(self, stream_id: int, params: dict) -> "PrepareMessage":
        raise InternalDriverError("subclass should implement method")

    def options(self, stream_id: int, params: dict) -> "OptionsMessage":
        raise InternalDriverError("subclass should implement method")

    def register(self, stream_id: int, params: dict) -> "RegisterMessage":
        raise InternalDriverError("subclass should implement method")
