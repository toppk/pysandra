from typing import Callable, Dict, Optional, Tuple

from .constants import CQL_VERSION, SERVER_SENT, Opcode, Options
from .exceptions import ServerError  # noqa: F401
from .exceptions import (
    InternalDriverError,
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
    SchemaResultMessage,
    StartupMessage,
    STypes,
    VoidResultMessage,
    get_struct,
)
from .types import ExpectedResponses  # noqa: F401
from .utils import SBytes, get_logger

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
        return StartupMessage(self.options, self.version, self.flags(), stream_id)

    def query(self, stream_id: int = None, params: dict = None) -> "QueryMessage":
        assert params is not None
        return QueryMessage(params["query"], self.version, self.flags(), stream_id,)

    def prepare(self, stream_id: int = None, params: dict = None) -> "PrepareMessage":
        assert params is not None
        return PrepareMessage(params["query"], self.version, self.flags(), stream_id,)

    def execute(self, stream_id: int = None, params: dict = None) -> "ExecuteMessage":
        assert params is not None
        statement_id = params["statement_id"]
        if statement_id not in self._prepared:
            raise InternalDriverError(
                f"missing statement_id={statement_id} in prepared statements"
            )
        prepared = self._prepared[statement_id]
        logger.debug(f"have prepared col_specs={prepared.col_specs}")
        return ExecuteMessage(
            statement_id,
            params["query_params"],
            prepared.col_specs,
            self.version,
            self.flags(),
            stream_id,
        )

    def build_response(
        self,
        request: "RequestMessage",
        version: int,
        flags: int,
        stream_id: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> "ExpectedResponses":
        sbytes_body = SBytes(body)
        response: Optional["ResponseMessage"] = None
        factory: Optional[Callable] = None
        if opcode == Opcode.ERROR:
            factory = ErrorMessage.build
        elif opcode == Opcode.READY:
            factory = ReadyMessage.build
        elif opcode == Opcode.AUTHENTICATE:
            pass
        elif opcode == Opcode.SUPPORTED:
            pass
        elif opcode == Opcode.RESULT:
            factory = ResultMessage.build
        elif opcode == Opcode.EVENT:
            pass
        elif opcode == Opcode.AUTH_CHALLENGE:
            pass
        elif opcode == Opcode.AUTH_SUCCESS:
            pass
        if factory is None:
            raise UnknownPayloadException(f"unhandled message opcode={opcode}")
        response = factory(version, flags, stream_id, sbytes_body)
        if response is None:
            raise InternalDriverError(
                f"didn't generate a response message for opcode={opcode}"
            )
        if not sbytes_body.at_end():
            raise InternalDriverError(f"still data left remains={sbytes_body.show()!r}")
        # error can happen any time
        if opcode == Opcode.ERROR:
            assert isinstance(response, ErrorMessage)
            raise ServerError(
                f'got error_code={response.error_code:x} with description="{response.error_text}"',
                msg=response,
            )
        return self.respond(request, response)

    def respond(
        self, request: "RequestMessage", response: "ResponseMessage"
    ) -> "ExpectedResponses":
        if request.opcode == Opcode.STARTUP:
            if response.opcode == Opcode.READY:
                return True
        elif request.opcode == Opcode.QUERY:
            if response.opcode == Opcode.RESULT:
                if isinstance(response, VoidResultMessage):
                    return True
                elif isinstance(response, RowsResultMessage):
                    return response.rows
                elif isinstance(response, SchemaResultMessage):
                    return response.schema_change
        elif request.opcode == Opcode.PREPARE:
            if response.opcode == Opcode.RESULT:
                if isinstance(response, PreparedResultMessage):
                    self._prepared[response.statement_id] = response
                    return response.statement_id
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
