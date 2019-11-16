
import asyncio

from .v4protocol import V4Protocol
from .exceptions import MaximumStreamsException, InternalDriverError

class Client:
    def __init__(self):
        self._proto = V4Protocol()
        self._streams = {}
        self._last_stream_id = None
        self._reader = None
        self._writer = None

    @property
    def protocol(self):
        return self._proto

    def _rm_stream_id(self, stream_id):
        if stream_id not in self._streams:
            raise InternalDriverError(f"stream_id={stream_id} is not open", stream_id=stream_id)
        del self._streams[stream_id]
        
    def _new_stream_id(self):
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
        self._streams[next_id] = True
        self._last_stream_id = next_id
        return next_id


    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(*default_host())
        send = self.protocol.startup(stream_id=self._new_stream_id())
        self._writer.write(send)
        head = await self._reader.read(9)
        version, flags, stream, opcode, length = self.protocol.decode(head)
        body = await self._reader.read(length)
        self._rm_stream_id(stream)
        self.protocol.decode_body(body)

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()
       
    async def query(self, query):
        send = self.protocol.query(query, stream_id=self._new_stream_id())
        self._writer.write(send)
        head = await self._reader.read(9)
        version, flags, stream, opcode, length = self.protocol.decode(head)
        body = await self._reader.read(length)
        self._rm_stream_id(stream)
    
        text = self.protocol.decode_body(body)
        print(text)
        return []
     

def default_host():
    return '127.0.0.1', 9042



if __name__ == '__main__':
    
    client = Client()
    move = 0
    while True:
        move += 1
        try:
            streamid = client.newstreamid()
        except MaximumStreamsException as e:
            print( len(client._streams))
            raise e
        print("got new streamid=%s" % streamid)
        if ( move % 19 ) == 0:
            
            print("remove streamid = %s" % streamid)
            client.closestreamid(streamid)
