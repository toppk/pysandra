import asyncio
from typing import Tuple

from .constants import REQUEST_TIMEOUT, STARTUP_TIMEOUT
from .dispatcher import Dispatcher
from .exceptions import RequestTimeout, StartupTimeout
from .protocol import Protocol
from .types import ExpectedResponses  # noqa: F401
from .utils import get_logger
from .v4protocol import V4Protocol

logger = get_logger(__name__)


class Client:
    def __init__(self) -> None:
        self._proto = V4Protocol()
        self._dispatcher = Dispatcher(protocol=self._proto, **default_host())
        self._is_ready = False
        self._in_startup = False
        self._is_ready_event = asyncio.Event()

    @property
    def protocol(self) -> "Protocol":
        return self._proto

    async def connect(self) -> None:
        if not self._is_ready:
            await self._startup()
            try:
                await asyncio.wait_for(
                    self._is_ready_event.wait(), timeout=STARTUP_TIMEOUT
                )
            except asyncio.TimeoutError as e:
                raise StartupTimeout(e) from None

    async def _startup(self) -> None:
        if self._in_startup:
            return
        self._in_startup = True
        logger.debug(" in _startup")
        event = await self._dispatcher.send(
            self.protocol.startup, self.protocol.build_response
        )
        try:
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError as e:
            raise RequestTimeout(e) from None

        is_ready = self._dispatcher.retrieve(event)
        logger.debug(f"startup is_ready={is_ready!r}")
        if is_ready:
            self._is_ready_event.set()

    async def close(self) -> None:
        await self._dispatcher.close()

    async def execute(self, stmt: str, args: Tuple = None) -> "ExpectedResponses":
        await self.connect()
        if args is None:
            # query
            event = await self._dispatcher.send(
                self.protocol.query,
                self.protocol.build_response,
                params={"query": stmt},
            )
            try:
                await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
                return self._dispatcher.retrieve(event)
            except asyncio.TimeoutError as e:
                raise RequestTimeout(e) from None
        else:
            # execute (prepared statements)
            event = await self._dispatcher.send(
                self.protocol.execute,
                self.protocol.build_response,
                params={"statement_id": stmt, "query_params": args},
            )
            try:
                await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
                return self._dispatcher.retrieve(event)
            except asyncio.TimeoutError as e:
                raise RequestTimeout(e) from None

    async def prepare(self, stmt: str) -> bytes:
        await self.connect()
        event = await self._dispatcher.send(
            self.protocol.prepare, self.protocol.build_response, params={"query": stmt}
        )
        try:
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
            resp = self._dispatcher.retrieve(event)
            assert isinstance(resp, bytes)
            return resp
        except asyncio.TimeoutError as e:
            raise RequestTimeout(e) from None


def default_host() -> dict:
    return {"host": "127.0.0.1", "port": 9042}
