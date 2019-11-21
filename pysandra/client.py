import asyncio
from os import getpid
from signal import Signals
from typing import List, Optional, Tuple

from .constants import REQUEST_TIMEOUT, STARTUP_TIMEOUT, Events  # noqa: F401
from .dispatcher import Dispatcher
from .exceptions import RequestTimeout, StartupTimeout, TypeViolation
from .protocol import Protocol
from .types import Connection, ExpectedResponses  # noqa: F401
from .utils import get_logger
from .v4protocol import V4Protocol

logger = get_logger(__name__)


class Client:
    def __init__(self, debug_signal: Optional["Signals"] = None) -> None:
        self._proto = V4Protocol()
        self._conn = Connection()
        self._dispatcher = Dispatcher(self._proto, self._conn)
        self._is_ready = False
        self._in_startup = False
        self._is_ready_event = asyncio.Event()
        if debug_signal is not None:
            self._install_signal(debug_signal)

    def _dump_state(self) -> None:
        logger.debug("in signal handler")

    def _install_signal(self, debug_signal: "Signals") -> None:
        if isinstance(debug_signal, Signals):
            signal = debug_signal
        elif isinstance(signal, str):
            try:
                signal = Signals(debug_signal)
            except ValueError:
                raise TypeViolation(
                    f"signal={debug_signal} is not valid.  Please use signal.SIG*"
                )

        loop = asyncio.get_event_loop()
        logger.debug(f" adding debug handler at signal={signal!r} for pid={getpid()}")
        loop.add_signal_handler(signal, self._dump_state)

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
        logger.debug(" sending Options")
        event = await self._dispatcher.send(
            self.protocol.options, self.protocol.build_response
        )
        try:
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError as e:
            raise RequestTimeout(e) from None
        supported_options = self._dispatcher.retrieve(event)
        assert isinstance(supported_options, dict)
        self._conn.make_choices(supported_options)
        logger.debug(f" sending Startup options={self._conn.options}")
        params = {"options": self._conn.options}
        event = await self._dispatcher.send(
            self.protocol.startup, self.protocol.build_response, params=params
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

    async def register(self, events: List["Events"]) -> "asyncio.Queue":
        await self.connect()

        event = await self._dispatcher.send(
            self.protocol.register,
            self.protocol.build_response,
            params={"events": events},
        )
        try:
            await asyncio.wait_for(event.wait(), timeout=REQUEST_TIMEOUT)
            resp = self._dispatcher.retrieve(event)
            assert isinstance(resp, asyncio.Queue)
            return resp
        except asyncio.TimeoutError as e:
            raise RequestTimeout(e) from None

    async def execute(
        self, stmt: str, args: Tuple = None, send_metadata: bool = False
    ) -> "ExpectedResponses":
        await self.connect()
        if args is None:
            # query
            event = await self._dispatcher.send(
                self.protocol.query,
                self.protocol.build_response,
                params={"query": stmt, "send_metadata": send_metadata},
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
                params={
                    "statement_id": stmt,
                    "query_params": args,
                    "send_metadata": send_metadata,
                },
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
