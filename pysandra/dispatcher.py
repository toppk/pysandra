import asyncio
from typing import Callable, Dict, Optional, Tuple, Union

from .constants import EVENT_STREAM_ID, Flags
from .core import Streams
from .exceptions import InternalDriverError, ServerError
from .messages import ErrorMessage, RequestMessage  # noqa: F401
from .protocol import Protocol  # noqa: F401
from .types import ExpectedResponses  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)


class Dispatcher:
    def __init__(
        self,
        protocol: "Protocol",
        reader: "asyncio.StreamReader",
        writer: "asyncio.StreamWriter",
    ) -> None:
        self._proto = protocol
        self._data: Dict[
            "asyncio.Event",
            Union["ExpectedResponses", "InternalDriverError", "ServerError"],
        ] = {}
        self._streams: Streams[
            Tuple["RequestMessage", Callable, asyncio.Event]
        ] = Streams()
        self.decompress: Optional[Callable] = None
        self._writer: "asyncio.StreamWriter" = writer
        self._reader: "asyncio.StreamReader" = reader
        self._read_task: Optional["asyncio.Future"] = None

    async def send(
        self, request_handler: Callable, response_handler: Callable, params: dict = None
    ) -> "asyncio.Event":
        stream_id = self._streams.create()
        request = request_handler(stream_id, params)
        event = asyncio.Event()
        self._streams.update(stream_id, (request, response_handler, event))
        assert self._writer is not None
        self._writer.write(bytes(request))
        return event

    def _process(
        self,
        version: int,
        flags: int,
        stream_id: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> None:
        if stream_id == EVENT_STREAM_ID:
            self._proto.event_handler(version, flags, stream_id, opcode, length, body)
        else:
            request, response_handler, event = self._streams.remove(stream_id)
            # exceptions are stashed here (in the wrong task)
            try:
                self._data[event] = response_handler(
                    request, version, flags, stream_id, opcode, length, body
                )
            except ServerError as e:
                self._data[event] = e
            except InternalDriverError as e:
                self._data[event] = e
            event.set()

    async def cycle(self) -> None:
        data = await self._receive(self._proto.decode_header)
        self._process(*data)

    async def _receive(
        self, decoder: Callable
    ) -> Tuple[int, int, int, int, int, bytes]:
        assert self._reader is not None
        head = await self._reader.readexactly(9)
        logger.debug(f"length of header={len(head)} at_eof={self._reader.at_eof()}")
        version, flags, stream_id, opcode, length = decoder(head)
        body = await self._reader.readexactly(length)
        logger.debug(f" got response head={head!r} body={body!r}")
        if flags & Flags.COMPRESSION:
            logger.debug(f"body={body!r}")
            assert self.decompress is not None
            body = self.decompress(body)
            logger.debug(f"body={body!r}")
        return version, flags, stream_id, opcode, length, body

    async def end_all(
        self, exception: Union["InternalDriverError", "ServerError"]
    ) -> None:
        # logger.warning(f" connection dropped, going to close")
        # close all requests
        for stream_id in self._streams.items():
            _req, _resp_handler, event = self._streams.remove(stream_id)
            self._data[event] = exception
            event.set()

    def retrieve(self, event: "asyncio.Event") -> "ExpectedResponses":
        try:
            response = self._data.pop(event)
            # exceptions are raised here (in the correct task)
            if isinstance(response, ServerError) or isinstance(
                response, InternalDriverError
            ):
                raise response
            return response
        except KeyError:
            raise InternalDriverError(f"missing data for event={event}")

    async def close(self, from_listener: bool = False) -> None:
        # cannot use wait_closed for 3.6 compatability
        # await self._writer.wait_closed()
        if self._writer is not None:
            self._writer.close()
