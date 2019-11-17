
import asyncio

from .dispatcher import Dispatcher
from .v4protocol import V4Protocol
from .constants import STARTUP_TIMEOUT, REQUEST_TIMEOUT
from .exceptions import MaximumStreamsException, InternalDriverError, StartupTimeout, RequestTimeout
from .utils import get_logger

logger = get_logger(__name__)

class Client:
    def __init__(self):
        self._proto = V4Protocol()
        self._dispatcher = Dispatcher(**default_host(), protocol=self._proto)
        self._is_ready = False
        self._in_startup = False
        self._is_ready_event = asyncio.Event()


    @property
    def protocol(self):
        return self._proto

    async def is_ready(self):
        if not self._is_ready:
            await self._startup()
            try:
                await asyncio.wait_for(self._is_ready_event.wait(), timeout=STARTUP_TIMEOUT)
            except asyncio.TimeoutError as e:
                raise StartupTimeout(e) from None
        return True


    async def _startup(self):
        if self._in_startup:
            return
        self._in_startup = True
        logger.debug(" in _startup")
        event = await self._dispatcher.send(self.protocol.startup, self.protocol.build_response)
        try: 
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError as e:
                raise RequestTimeout(e) from None
            
        is_ready = self._dispatcher.retrieve(event)
        logger.debug(f"startup is_ready={is_ready}")
        if is_ready:
            self._is_ready_event.set()
       

    async def close(self):
        await self._dispatcher.close()
       
    async def query(self, query):
        await self.is_ready()
        event = await self._dispatcher.send(self.protocol.query, self.protocol.build_response, params={'query': query})
        try: 
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
            return self._dispatcher.retrieve(event)
        except asyncio.TimeoutError as e:
                raise RequestTimeout(e) from None


def default_host():
    return {"host": '127.0.0.1',
            "port": 9042}



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
