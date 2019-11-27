import asyncio
import ssl
import sys
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from .constants import (
    CQL_VERSION,
    DEFAULT_HOST,
    DEFAULT_PORT,
    PREFERRED_ALGO,
    REQUEST_TIMEOUT,
    Options,
)
from .dispatcher import Dispatcher
from .exceptions import ConnectionDroppedError, InternalDriverError, RequestTimeout
from .types import ExpectedResponses  # noqa: F401
from .utils import PKZip, get_logger
from .v4protocol import V4Protocol

logger = get_logger(__name__)


class Connection:
    def __init__(
        self,
        host: Tuple[str, int] = None,
        use_tls: bool = False,
        no_compress: bool = False,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        self.protocol = V4Protocol()
        if options is None:
            options = {Options.CQL_VERSION: CQL_VERSION}

        self.host = host[0] if host is not None else DEFAULT_HOST
        self.port = host[1] if host is not None else DEFAULT_PORT
        self.no_compress = no_compress
        self.tls: Optional["ssl.SSLContext"] = None
        if use_tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_NONE
            context.check_hostname = False
            self.tls = context
        self.preferred_algo = PREFERRED_ALGO
        self._compress: Optional[Callable] = None
        self._decompress: Optional[Callable] = None
        self._read_task: Optional["asyncio.Future"] = None
        self._is_ready = False
        self._in_startup = False
        self._is_connected = False
        self._options = options
        self.supported_options: Optional[Dict[str, List[str]]] = None
        self._dispatcher: Optional["Dispatcher"] = None

    @property
    def is_connected(self) -> bool:
        logger.debug(f"is_connected={self._is_connected}")
        return self._is_connected

    @property
    def is_ready(self) -> bool:
        return self._is_ready

    async def make_call(
        self,
        request_handler: Callable,
        response_handler: Callable,
        params: Dict[str, Any] = None,
    ) -> "ExpectedResponses":
        logger.debug(f" sending {request_handler}")
        assert self._dispatcher is not None
        event = await self._dispatcher.send(
            request_handler, response_handler, params=params
        )
        try:
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError as e:
            raise RequestTimeout(e) from None
        return self._dispatcher.retrieve(event)

    # should return typing.NoReturn
    async def _listener(self) -> None:
        assert self._dispatcher is not None
        try:
            while True:
                await self._dispatcher.cycle()
        except asyncio.IncompleteReadError as e:
            exp = ConnectionDroppedError(e)
            logger.warning(f" connection dropped, going to close")
            await self._dispatcher.end_all(exp)
        except ConnectionResetError as e:
            exp = ConnectionDroppedError(e)
            logger.warning(f" connection dropped, going to close")
            await self._dispatcher.end_all(exp)
        except asyncio.CancelledError:
            logger.debug(f"got CanceledError")
            await self.close(True)
            # raise (e)
        # do I know what I'm doing?
        except BaseException:
            logger.warning(f"got baseeception")
            traceback.print_exc(file=sys.stdout)

    async def startup(self) -> bool:
        if self._in_startup:
            return False
        self._in_startup = True
        reader, writer = await asyncio.open_connection(
            self.host, self.port, ssl=self.tls
        )
        self._is_connected = True
        self._dispatcher = Dispatcher(self.protocol, reader, writer)
        # avoid create_task for 3.6 compatability
        self._read_task = asyncio.ensure_future(self._listener())
        supported_options = await self.make_call(
            self.protocol.options, self.protocol.build_response
        )
        assert isinstance(supported_options, dict)
        self.make_choices(supported_options)
        logger.debug(f" sending Startup options={self.options}")
        params = {"options": self.options}
        # READY may be compressed
        if "COMPRESSION" in self.options:
            logger.debug(f"setting dec2omress to algo={self.decompress}")
            self._dispatcher.decompress = self._decompress
            self.protocol.compress = self._compress
        is_ready = await self.make_call(
            self.protocol.startup, self.protocol.build_response, params=params
        )
        assert isinstance(is_ready, bool) and is_ready
        self._is_ready = is_ready
        logger.debug(f"startup is_ready={is_ready}")
        return is_ready

    async def close(self, from_listener: bool = False) -> None:

        if self._dispatcher is not None:
            await self._dispatcher.close()
        if self._read_task is not None and not from_listener:
            self._read_task.cancel()

    def make_choices(self, supported_options: Dict[str, List[str]]) -> None:
        self.supported_options = supported_options
        if self.supported_options is not None:
            # check options
            # set compression
            pkzip = PKZip()
            if "COMPRESSION" in self.supported_options and not self.no_compress:
                matches = [
                    algo
                    for algo in pkzip.supported
                    if algo in self.supported_options["COMPRESSION"]
                ]
                if len(matches) > 0:
                    select = (
                        self.preferred_algo
                        if self.preferred_algo in matches
                        else matches[0]
                    )
                    self._options["COMPRESSION"] = select
                    self._compress = pkzip.get_compress(select)
                    self._decompress = pkzip.get_decompress(select)

    def decompress(self, data: bytes) -> bytes:
        if self._decompress is None:
            raise InternalDriverError("no compression selected")
        return self._decompress(data)

    def compress(self, data: bytes) -> bytes:
        if self._compress is None:
            raise InternalDriverError("no compression selected")
        return self._compress(data)

    @property
    def options(self) -> Dict[str, str]:
        return self._options
