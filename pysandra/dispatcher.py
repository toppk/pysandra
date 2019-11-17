import asyncio
import traceback
import sys

from .utils import get_logger

logger = get_logger(__name__)


class Dispatcher:
    def __init__(self, host=None, port=None, protocol=None):
        self._host = host
        self._port = port
        self._proto = protocol
        self._reader = None
        self._data = {}
        self._writer = None
        self._streams = {}
        self._last_stream_id = None
        self._connected = False
        self._running = False

    def _rm_stream_id(self, stream_id):
        try:
            return self._streams.pop(stream_id)
        except KeyError:
            raise InternalDriverError(
                f"stream_id={stream_id} is not open", stream_id=stream_id
            )

    def _new_stream_id(self):
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

    def _update_stream_id(self, stream_id, store):
        if stream_id not in self._streams:
            raise InternalDriverError(f"stream_id={stream_id} not being tracked")
        if store is None:
            raise InternalDriverError("cannot store empty request")
        self._streams[stream_id] = store

    async def send(self, request_handler, response_handler, params=None):
        if not self._connected:
            await self._connect()

        stream_id = self._new_stream_id()
        request = request_handler(stream_id=stream_id, params=params)
        event = asyncio.Event()
        self._update_stream_id(stream_id, (request, response_handler, event))
        self._writer.write(request.to_bytes())
        return event

    async def _receive(self):
        head = await self._reader.read(9)
        logger.debug(f"in _receive head={head}")
        version, flags, stream, opcode, length = self._proto.decode_header(head)
        body = await self._reader.read(length)
        request, response_handler, event = self._rm_stream_id(stream)
        data = response_handler(request, version, flags, stream, opcode, length, body)
        self._data[event] = data
        event.set()

    def retrieve(self, event):
        try:
            return self._data.pop(event)
        except KeyError:
            raise InternalDriverError(f"missing data for event={event}")

    async def _listener(self):
        self._running = True
        try:
            while self._connected:
                await self._receive()
        except asyncio.CancelledError as e:
            if self._running:
                raise (e)
        except:
            traceback.print_exc(file=sys.stdout)

    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )
        self._connected = True
        self._read_task = asyncio.create_task(self._listener())

    async def close(self):
        self._writer.close()
        self._connected = False
        self._running = False
        self._read_task.cancel()
        await self._writer.wait_closed()
