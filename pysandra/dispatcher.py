import asyncio
import ssl
import sys
import traceback
from typing import Callable, Dict, List, Optional, Tuple, Union

from .constants import EVENT_STREAM_ID, Flags
from .exceptions import (
    ConnectionDroppedError,
    InternalDriverError,
    MaximumStreamsException,
    ServerError,
)
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
        self._streams: Dict[
            int, Optional[Tuple["RequestMessage", Callable, asyncio.Event]]
        ] = {}
        self.decompress: Optional[Callable] = None
        self._connected = False
        self._running = False
        self._last_stream_id: Optional[int] = None
        self._writer: Optional["asyncio.StreamWriter"] = None
        self._reader: Optional["asyncio.StreamReader"] = None
        self._read_task: Optional["asyncio.Future"] = None

    def _list_stream_ids(self) -> List[int]:
        return list(self._streams.keys())

    def _rm_stream_id(
        self, stream_id: int
    ) -> Tuple["RequestMessage", Callable, asyncio.Event]:
        try:
            store = self._streams.pop(stream_id)
            assert store is not None
            return store
        except KeyError:
            raise InternalDriverError(
                f"stream_id={stream_id} is not open", stream_id=stream_id
            )

    def _new_stream_id(self) -> int:
        maxstream = 2 ** 15
        last_id = self._last_stream_id
        if last_id is None:
            next_id = 0x00
        elif len(self._streams) > maxstream:
            raise MaximumStreamsException(
                f"too many streams last_id={last_id} length={len(self._streams)}"
            )
        else:
            next_id = last_id + 1
            while True:
                if next_id > maxstream:
                    next_id = 0x00
                if next_id not in self._streams:
                    break
                # print("cannot use %s" % next_id)
                next_id = next_id + 1
        if next_id is None:
            raise InternalDriverError("next_id cannot be None")
        # store will come in later
        self._streams[next_id] = None
        self._last_stream_id = next_id
        return next_id

    def _update_stream_id(
        self, stream_id: int, store: Tuple["RequestMessage", Callable, asyncio.Event]
    ) -> None:
        if stream_id not in self._streams:
            raise InternalDriverError(f"stream_id={stream_id} not being tracked")
        if store is None:
            raise InternalDriverError("cannot store empty request")
        self._streams[stream_id] = store

    async def send(
        self, request_handler: Callable, response_handler: Callable, params: dict = None
    ) -> "asyncio.Event":
        if not self._connected:
            await self._connect()

        stream_id = self._new_stream_id()
        # should order compression
        request = request_handler(stream_id, params)
        event = asyncio.Event()
        self._update_stream_id(stream_id, (request, response_handler, event))
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
            request, response_handler, event = self._rm_stream_id(stream_id)
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
            for stream_id in self._list_stream_ids():
                _req, _resp_handler, event = self._rm_stream_id(stream_id)
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
