from typing import Dict, Optional, Tuple, Union

from .constants import CQL_VERSION, SERVER_SENT, Opcode, Options
from .exceptions import (
    InternalDriverError,
    ServerError,
    UnknownPayloadException,
    VersionMismatchException,
)
from .protocol import (
    NETWORK_ORDER,
    ErrorMessage,
    ExecuteMessage,
    PreparedResultMessage,
    PrepareMessage,
    Protocol,
    QueryMessage,
    ReadyMessage,
    RequestMessage,
    ResponseMessage,
    ResultMessage,
    RowsResultMessage,
    StartupMessage,
    STypes,
    VoidResultMessage,
    get_struct,
)
from .types import Rows  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)


class V4Protocol(Protocol):
    version = 0x04

    def __init__(self, default_flags: int = 0x00) -> None:
        self._default_flags = default_flags
        self._prepared: Dict[bytes, "PreparedResultMessage"] = {}

    def reset_connection(self) -> None:
        self._prepared = {}

    @property
    def options(self) -> dict:
        return {Options.CQL_VERSION: CQL_VERSION}

    def flags(self, flags: int = None) -> int:
        if flags is None:
            flags = self._default_flags
        return flags

    def startup(self, stream_id: int = None, params: dict = None) -> "StartupMessage":
        return StartupMessage(
            version=self.version,
            flags=self.flags(),
            options=self.options,
            stream_id=stream_id,
        )

    def query(self, stream_id: int = None, params: dict = None) -> "QueryMessage":
        assert params is not None
        return QueryMessage(
            version=self.version,
            flags=self.flags(),
            query=params["query"],
            stream_id=stream_id,
        )

    def prepare(self, stream_id: int = None, params: dict = None) -> "PrepareMessage":
        assert params is not None
        return PrepareMessage(
            version=self.version,
            flags=self.flags(),
            query=params["query"],
            stream_id=stream_id,
        )

    def execute(self, stream_id: int = None, params: dict = None) -> "ExecuteMessage":
        assert params is not None
        query_id = params["query_id"]
        if query_id not in self._prepared:
            raise InternalDriverError(
                f"missing query_id={query_id} in prepared statements"
            )
        prepared = self._prepared[query_id]
        logger.debug(f"have prepared col_specs={prepared.col_specs}")
        return ExecuteMessage(
            version=self.version,
            flags=self.flags(),
            query_id=query_id,
            query_params=params["query_params"],
            col_specs=prepared.col_specs,
            stream_id=stream_id,
        )

    def build_response(
        self,
        request: "RequestMessage",
        version: int,
        flags: int,
        stream: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> Union[bool, "Rows", bytes]:
        response: Optional["ResponseMessage"] = None
        if opcode == Opcode.ERROR:
            response = ErrorMessage.build(version=version, flags=flags, body=body)
            raise ServerError(
                f'got error_code={response.error_code:x} with description="{response.error_text}"',
                msg=response,
            )
        elif opcode == Opcode.READY:
            response = ReadyMessage.build(version=version, flags=flags, body=body)
        elif opcode == Opcode.AUTHENTICATE:
            pass
        elif opcode == Opcode.SUPPORTED:
            pass
        elif opcode == Opcode.RESULT:
            response = ResultMessage.build(
                version=version, flags=flags, query_flags=request.flags, body=body
            )
        elif opcode == Opcode.EVENT:
            pass
        elif opcode == Opcode.AUTH_CHALLENGE:
            pass
        elif opcode == Opcode.AUTH_SUCCESS:
            pass
        else:
            raise UnknownPayloadException(f"unknown message opcode={opcode}")
        if response is None:
            raise InternalDriverError(
                f"didn't generate a response message for opcode={opcode}"
            )
        return self.respond(request, response)

    def respond(
        self, request: "RequestMessage", response: "ResponseMessage"
    ) -> Union[bool, "Rows", bytes]:
        if request.opcode == Opcode.STARTUP:
            if response.opcode == Opcode.READY:
                return True
        elif request.opcode == Opcode.QUERY:
            if response.opcode == Opcode.RESULT:
                if isinstance(response, VoidResultMessage):
                    return True
                elif isinstance(response, RowsResultMessage):
                    return response.rows
        elif request.opcode == Opcode.PREPARE:
            if response.opcode == Opcode.RESULT:
                if isinstance(response, PreparedResultMessage):
                    self._prepared[response.query_id] = response
                    return response.query_id
        elif request.opcode == Opcode.EXECUTE:
            if response.opcode == Opcode.RESULT:
                if isinstance(response, VoidResultMessage):
                    return True

        raise InternalDriverError(
            f"unhandled response={response} for request={request}"
        )

    def decode_header(self, header: bytes) -> Tuple[int, int, int, int, int]:
        version, flags, stream, opcode, length = get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).unpack(header)
        logger.debug(
            f"got head={header!r} containing version={version:x} flags={flags:x} stream={stream:x} opcode={opcode:x} length={length:x}"
        )
        expected_version = SERVER_SENT | self.version
        if version != expected_version:
            raise VersionMismatchException(
                f"received version={version:x} instead of expected_version={expected_version}"
            )
        return version, flags, stream, opcode, length
