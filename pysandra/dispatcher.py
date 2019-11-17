import asyncio

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

    def _rm_stream_id(self, stream_id):
        try:
            return self._streams.pop(stream_id)
        except KeyError:
            raise InternalDriverError(f"stream_id={stream_id} is not open", stream_id=stream_id)
        
    def _new_stream_id(self, response):
        maxstream = 2**15
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
                #print("cannot use %s" % next_id)
                next_id = next_id + 1
        if next_id is None:
            raise InternalDriverError("next_id cannot be None")
        event = asyncio.Event()
        self._streams[next_id] = (response, event)
        self._last_stream_id = next_id
        return next_id, event

    async def send(self, request, response, request_args=[]):
        if not self._connected:
            await self._connect()

        stream_id, event = self._new_stream_id(response)
        send = request(*request_args, stream_id=stream_id)
        self._writer.write(send)
        return event
 

    async def _receive(self):
        head = await self._reader.read(9)
        logger.debug(f"in _receive head={head}")
        version, flags, stream, opcode, length = self._proto.decode_header(head)
        body = await self._reader.read(length)
        response, event = self._rm_stream_id(stream)
        data = response(version, flags, stream, opcode, length, body)
        self._data[event] = data
        event.set()

    def retrieve(self, event):
        try:
            return self._data.pop(event)
        except KeyError:
            raise InternalDriverError(f"missing data for event={event}")

    async def _listener(self):
        while self._connected:
            await self._receive()
            
    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._connected = True
        self._read_task = asyncio.create_task(self._listener())

    async def close(self):
        self._writer.close()
        self._connected = False
        self._read_task.cancel()
        await self._writer.wait_closed()
  
