import asyncio
import ssl
import sys
import traceback
from typing import Callable, Dict, Optional, Tuple, Union

from .constants import EVENT_STREAM_ID, Flags
from .core import Streams
from .exceptions import ConnectionDroppedError, InternalDriverError, ServerError
from .protocol import ErrorMessage, Protocol, RequestMessage  # noqa: F401
from .types import ExpectedResponses  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)


class Dispatcher:
    def __init__(
        self, protocol: "Protocol", host: str, port: int, use_tls: bool
    ) -> None:
        assert protocol is not None
        self._proto = protocol
        self._tls: Optional["ssl.SSLContext"] = None
        if use_tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_NONE
            context.check_hostname = False
            self._tls = context
        self._host = host
        self._port = port
        self._data: Dict[
            "asyncio.Event",
            Union["ExpectedResponses", "InternalDriverError", "ServerError"],
        ] = {}
        self._streams: Streams[
            Tuple["RequestMessage", Callable, asyncio.Event]
        ] = Streams()
        self.decompress: Optional[Callable] = None
        self._connected = False
        self._running = False
        self._writer: Optional["asyncio.StreamWriter"] = None
        self._reader: Optional["asyncio.StreamReader"] = None
        self._read_task: Optional["asyncio.Future"] = None

    async def send(
        self, request_handler: Callable, response_handler: Callable, params: dict = None
    ) -> "asyncio.Event":
        if not self._connected:
            await self._connect()

        stream_id = self._streams.create()
        # should order compression
        request = request_handler(stream_id, params)
        event = asyncio.Event()
        self._streams.update(stream_id, (request, response_handler, event))
        assert self._writer is not None
        self._writer.write(bytes(request))
        return event

    async def _receive(self) -> None:
        assert self._reader is not None
        try:
            head = await self._reader.read(9)
        except ConnectionResetError as e:
            raise ConnectionDroppedError(e) from None
        logger.debug(f"in _receive head={head!r}")
        version, flags, stream_id, opcode, length = self._proto.decode_header(head)
        body = await self._reader.read(length)
        # should decompress
        if flags & Flags.COMPRESSION:
            logger.debug(f"body={body!r}")
            assert self.decompress is not None
            body = self.decompress(body)
            logger.debug(f"body={body!r}")
        if stream_id == EVENT_STREAM_ID:
            await self._proto.event_handler(
                version, flags, stream_id, opcode, length, body
            )
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

    # should return typing.NoReturn
    async def _listener(self) -> None:
        self._running = True
        try:
            while self._connected:
                await self._receive()
        except ConnectionDroppedError as e:
            # logger.warning(f" connection dropped, going to close")
            self._running = False
            # close all requests
            for stream_id in self._streams.items():
                _req, _resp_handler, event = self._streams.remove(stream_id)
                self._data[event] = e
                event.set()
            self._reader = None
            self._writer = None
        except asyncio.CancelledError as e:
            logger.debug(f"got canceled error running=[{self._running}] e=[{e}]")
            if self._running:
                await self.close(True)
                # raise (e)
        # do I know what I'm doing?
        except BaseException:
            logger.warning(f"got baseeception")
            traceback.print_exc(file=sys.stdout)

    async def _connect(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port, ssl=self._tls
        )
        self._connected = True
        # avoid create_task for 3.6 compatability
        self._read_task = asyncio.ensure_future(self._listener())

    async def close(self, from_listener: bool = False) -> None:
        if self._writer is not None:
            self._writer.close()
        self._connected = False
        self._running = False
        if self._read_task is not None and not from_listener:
            self._read_task.cancel()
        # cannot use wait_closed for 3.6 compatability
        # await self._writer.wait_closed()
