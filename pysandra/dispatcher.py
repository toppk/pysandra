import asyncio
import sys
import traceback
from typing import Callable, Dict, Optional, Tuple, Union

from .exceptions import InternalDriverError, MaximumStreamsException, ServerError
from .protocol import ErrorMessage, Protocol, RequestMessage  # noqa: F401
from .types import ExpectedResponses  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)


class Dispatcher:
    def __init__(
        self, host: str = None, port: int = None, protocol: "Protocol" = None
    ) -> None:
        self._host = host
        self._port = port
        assert protocol is not None
        self._proto = protocol
        self._data: Dict[
            "asyncio.Event",
            Union["ExpectedResponses", "InternalDriverError", "ServerError"],
        ] = {}
        self._streams: Dict[
            int, Optional[Tuple["RequestMessage", Callable, asyncio.Event]]
        ] = {}
        self._connected = False
        self._running = False
        self._last_stream_id: Optional[int] = None
        self._writer: Optional["asyncio.StreamWriter"] = None
        self._reader: Optional["asyncio.StreamReader"] = None
        self._read_task: Optional["asyncio.Future"] = None

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
            raise MaximumStreamsException
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
        request = request_handler(stream_id=stream_id, params=params)
        event = asyncio.Event()
        self._update_stream_id(stream_id, (request, response_handler, event))
        assert self._writer is not None
        self._writer.write(request.to_bytes())
        return event

    async def _receive(self) -> None:
        assert self._reader is not None
        head = await self._reader.read(9)
        logger.debug(f"in _receive head={head!r}")
        version, flags, stream, opcode, length = self._proto.decode_header(head)
        body = await self._reader.read(length)
        request, response_handler, event = self._rm_stream_id(stream)
        # exceptions are stashed here (in the wrong task)
        try:
            self._data[event] = response_handler(
                request, version, flags, stream, opcode, length, body
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
        except asyncio.CancelledError as e:
            if self._running:
                raise (e)
        # do I know what I'm doing?
        except BaseException:
            traceback.print_exc(file=sys.stdout)

    async def _connect(self) -> None:
        assert self._host is not None
        assert self._port is not None
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        self._connected = True
        # avoid create_task for 3.6 compatability
        self._read_task = asyncio.ensure_future(self._listener())

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        self._connected = False
        self._running = False
        if self._read_task is not None:
            self._read_task.cancel()
        # cannot use wait_closed for 3.6 compatability
        # await self._writer.wait_closed()


if __name__ == "__main__":

    client = Dispatcher()
    move = 0
    while True:
        move += 1
        try:
            streamid = client._new_stream_id()
        except MaximumStreamsException as e:
            print(len(client._streams))
            raise e
        print("got new streamid=%s" % streamid)
        if (move % 19) == 0:

            print("remove streamid = %s" % streamid)
            client._rm_stream_id(streamid)
