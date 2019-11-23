import asyncio
from typing import Callable, Dict, List, Optional, Tuple

from .constants import (
    CQL_VERSION,
    DEFAULT_HOST,
    DEFAULT_PORT,
    PREFERRED_ALGO,
    REQUEST_TIMEOUT,
    Options,
)
from .dispatcher import Dispatcher
from .exceptions import InternalDriverError, RequestTimeout
from .types import ExpectedResponses  # noqa: F401
from .utils import PKZip, get_logger
from .v4protocol import V4Protocol

logger = get_logger(__name__)


class Connection:
    def __init__(
        self,
        host: Tuple[str, int] = None,
        use_tls: bool = False,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        self.protocol = V4Protocol()
        if options is None:
            options = {Options.CQL_VERSION: CQL_VERSION}

        self.host = host[0] if host is not None else DEFAULT_HOST
        self.port = host[1] if host is not None else DEFAULT_PORT
        self.use_tls = use_tls
        self.preferred_algo = PREFERRED_ALGO
        self.is_ready = False
        self._in_startup = False
        self._options = options
        self._pkzip = PKZip()
        self.supported_options: Optional[Dict[str, List[str]]] = None
        self._dispatcher = Dispatcher(self.protocol, self.host, self.port, self.use_tls)

    async def make_call(
        self, request_handler: Callable, response_handler: Callable, params: dict = None
    ) -> "ExpectedResponses":
        logger.debug(f" sending {request_handler}")
        event = await self._dispatcher.send(
            request_handler, response_handler, params=params
        )
        try:
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError as e:
            raise RequestTimeout(e) from None
        return self._dispatcher.retrieve(event)

    async def startup(self) -> bool:
        if self._in_startup:
            return False
        self._in_startup = True
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
            self._dispatcher.decompress = self.decompress
            self.protocol.compress = self.compress
        is_ready = await self.make_call(
            self.protocol.startup, self.protocol.build_response, params=params
        )
        assert isinstance(is_ready, bool) and is_ready
        self.is_ready = is_ready
        logger.debug(f"startup is_ready={is_ready}")
        return is_ready

    async def close(self) -> None:
        await self._dispatcher.close()

    def decompress(self, data: bytes) -> bytes:
        if "COMPRESSION" not in self._options:
            raise InternalDriverError(f"no compression selected")
        return self._pkzip.decompress(data, self._options["COMPRESSION"])

    def compress(self, data: bytes) -> bytes:
        if "COMPRESSION" not in self._options:
            raise InternalDriverError(f"no compression selected")
        return self._pkzip.compress(data, self._options["COMPRESSION"])

    def make_choices(self, supported_options: Dict[str, List[str]]) -> None:
        self.supported_options = supported_options
        if self.supported_options is not None:
            # check options
            # set compression
            if "COMPRESSION" in self.supported_options:
                matches = [
                    algo
                    for algo in self._pkzip.supported
                    if algo in self.supported_options["COMPRESSION"]
                ]
                if len(matches) > 1:
                    select = (
                        self.preferred_algo
                        if self.preferred_algo in matches
                        else matches[0]
                    )
                    self._options["COMPRESSION"] = select

    @property
    def options(self) -> Dict[str, str]:
        return self._options
