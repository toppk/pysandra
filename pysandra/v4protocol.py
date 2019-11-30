from asyncio import Queue, QueueFull
from typing import Any, Callable, Dict, Optional, Type

from .constants import EVENTS_QUEUE_MAXSIZE, Opcode
from .core import SBytes
from .exceptions import ServerError  # noqa: F401
from .exceptions import InternalDriverError, UnknownPayloadException
from .messages import (
    ErrorMessage,
    EventMessage,
    ExecuteMessage,
    OptionsMessage,
    PreparedResultMessage,
    PrepareMessage,
    QueryMessage,
    ReadyMessage,
    RegisterMessage,
    RequestMessage,
    ResponseMessage,
    ResultMessage,
    RowsResultMessage,
    SchemaResultMessage,
    SetKeyspaceResultMessage,
    StartupMessage,
    SupportedMessage,
    VoidResultMessage,
)
from .protocol import Protocol
from .types import ExpectedResponses  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)


class V4Protocol(Protocol):
    version = 0x04

    def __init__(self, default_flags: int = 0x00, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._default_flags = default_flags
        self.compress: Optional[Callable] = None
        self._prepared: Dict[bytes, "PreparedResultMessage"] = {}
        self._events: Optional["Queue"] = None

    def reset_connection(self) -> None:
        self._prepared = {}

    def flags(self, flags: int = None) -> int:
        if flags is None:
            flags = self._default_flags
        return flags

    def options(self, stream_id: int, params: dict) -> "OptionsMessage":
        return OptionsMessage(self.version, self.flags(), stream_id)

    def startup(self, stream_id: int, params: dict) -> "StartupMessage":
        return StartupMessage(params["options"], self.version, self.flags(), stream_id)

    def query(self, stream_id: int, params: dict) -> "QueryMessage":
        assert params is not None
        return QueryMessage(
            params["query"],
            params["query_params"],
            params["send_metadata"],
            self.version,
            self.flags(),
            stream_id,
            compress=self.compress,
            consistency=params.get("consistency"),
        )

    def register(self, stream_id: int, params: dict) -> "RegisterMessage":
        assert params is not None
        return RegisterMessage(params["events"], self.version, self.flags(), stream_id)

    def prepare(self, stream_id: int, params: dict) -> "PrepareMessage":
        assert params is not None
        return PrepareMessage(
            params["query"],
            self.version,
            self.flags(),
            stream_id,
            compress=self.compress,
        )

    def execute(self, stream_id: int, params: dict) -> "ExecuteMessage":
        assert params is not None
        statement_id = params["statement_id"]
        if statement_id not in self._prepared:
            raise InternalDriverError(
                f"missing statement_id={statement_id} in prepared statements"
            )
        prepared = self._prepared[statement_id]
        logger.debug(
            f"have prepared col_specs={prepared.col_specs} statement_id={statement_id} and params={params}"
        )
        return ExecuteMessage(
            statement_id,
            params["query_params"],
            params["send_metadata"],
            prepared.col_specs,
            self.version,
            self.flags(),
            stream_id,
            compress=self.compress,
            consistency=params.get("consistency"),
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
        if self._events is None:
            raise InternalDriverError(f"got event={body!r} when no registered occured")
        sbytes_body = SBytes(body)
        msg = EventMessage.create(version, flags, stream_id, sbytes_body)
        assert isinstance(msg, EventMessage)
        try:
            self._events.put_nowait(msg.event)
        except QueueFull:
            raise InternalDriverError(f"event queue is full, droppping event")

    def build_response(
        self,
        request: "RequestMessage",
        version: int,
        flags: int,
        stream_id: int,
        opcode_int: int,
        length: int,
        body: bytes,
    ) -> "ExpectedResponses":
        sbytes_body = SBytes(body)
        response: Optional["ResponseMessage"] = None
        factory: Optional[Type["ResponseMessage"]] = None
        try:
            opcode = Opcode(opcode_int)
        except ValueError:
            raise InternalDriverError(f"unknown optcode={opcode_int}")
        if opcode == Opcode.ERROR:
            factory = ErrorMessage
        elif opcode == Opcode.READY:
            factory = ReadyMessage
        elif opcode == Opcode.AUTHENTICATE:
            pass
        elif opcode == Opcode.SUPPORTED:
            factory = SupportedMessage
        elif opcode == Opcode.RESULT:
            factory = ResultMessage
        elif opcode == Opcode.EVENT:
            pass
        elif opcode == Opcode.AUTH_CHALLENGE:
            pass
        elif opcode == Opcode.AUTH_SUCCESS:
            pass
        if factory is None:
            raise UnknownPayloadException(f"unhandled message opcode={opcode!r}")
        logger.debug(f"calling create on factory={factory}")
        response = factory.create(version, flags, stream_id, sbytes_body)
        # error can happen any time
        if opcode == Opcode.ERROR:
            assert isinstance(response, ErrorMessage)
            raise ServerError(
                f"received error_code={response.error_code:x} response from server",
                error_code=response.error_code,
                error_text=response.error_text,
                details=response.details,
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
                elif isinstance(response, SetKeyspaceResultMessage):
                    return response.keyspace
                elif isinstance(response, SchemaResultMessage):
                    return response.schema_change
        elif request.opcode == Opcode.OPTIONS:
            if response.opcode == Opcode.SUPPORTED:
                assert isinstance(response, SupportedMessage)
                return response.options
        elif request.opcode == Opcode.REGISTER:
            if response.opcode == Opcode.READY:
                if self._events is None:
                    self._events = Queue(maxsize=EVENTS_QUEUE_MAXSIZE)
                return self._events
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
            f"unhandled response={response.opcode!r} for request={request.opcode!r}"
        )
