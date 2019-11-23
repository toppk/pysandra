import asyncio
import signal
from collections.abc import Iterable
from os import getpid
from types import FrameType  # noqa: F401
from typing import Any, Callable, List, Optional, Tuple, Union

from .connection import Connection
from .constants import REQUEST_TIMEOUT, STARTUP_TIMEOUT, Events  # noqa: F401
from .exceptions import StartupTimeout, TypeViolation
from .protocol import Protocol  # noqa: F401
from .types import ExpectedResponses  # noqa: F401
from .utils import get_logger

logger = get_logger(__name__)


def online(f: Callable) -> Callable:
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        await args[0].connect()
        return await f(*args, **kwargs)

    return wrapper


class Client:
    def __init__(
        self,
        host: Optional[Tuple[str, int]] = None,
        use_tls: bool = False,
        debug_signal: Optional["signal.Signals"] = None,
    ) -> None:
        # this protocol will never be used, just placating mypy
        self._conn = Connection(host=host, use_tls=use_tls)
        self._in_startup = False
        self._is_ready_event = asyncio.Event()
        if debug_signal is not None:
            self._install_signal(debug_signal)

    def _dump_state(self, signum: int, frame: Optional["FrameType"]) -> None:
        logger.warning(f"Dumping Internal State signal={signum} and frame={frame}")

    def _install_signal(self, debug_signal: Union["signal.Signals", str, int]) -> None:
        debug_signal = debug_signal
        if isinstance(debug_signal, int):
            try:
                debug_signal = signal.Signals(debug_signal)
            except ValueError:
                raise TypeViolation(
                    f"debug signal is not valid signal={debug_signal}.  Please use signal.SIG*"
                )
        elif isinstance(debug_signal, str):
            try:
                debug_signal = signal.Signals[debug_signal]
            except KeyError:
                raise TypeViolation(
                    f"debug signal is not valid signal={debug_signal}.  Please use signal.SIG*"
                )

        logger.debug(f" adding debug handler at signal={signal!r} for pid={getpid()}")
        signal.signal(debug_signal, self._dump_state)
        # loop = asyncio.get_event_loop()
        # loop.add_signal_handler(signal, self._dump_state)

    @property
    def is_connected(self) -> bool:
        return self._conn.is_ready

    async def connect(self) -> None:
        if not self._conn.is_ready:
            if await self._conn.startup():
                self._is_ready_event.set()
            else:
                try:
                    await asyncio.wait_for(
                        self._is_ready_event.wait(), timeout=STARTUP_TIMEOUT
                    )
                except asyncio.TimeoutError as e:
                    raise StartupTimeout(e) from None

    async def close(self) -> None:
        await self._conn.close()

    @online
    async def register(self, events: List["Events"]) -> "asyncio.Queue":
        resp = await self._conn.make_call(
            self._conn.protocol.register,
            self._conn.protocol.build_response,
            params={"events": events},
        )

        assert isinstance(resp, asyncio.Queue)
        return resp

    @online
    async def execute(
        self, stmt: str, args: Optional[Iterable] = None, send_metadata: bool = False
    ) -> "ExpectedResponses":
        logger.debug(f" in execte got args={args}")
        if isinstance(stmt, str):
            # query
            return await self._conn.make_call(
                self._conn.protocol.query,
                self._conn.protocol.build_response,
                params={
                    "query": stmt,
                    "query_params": args,
                    "send_metadata": send_metadata,
                },
            )
        else:
            # execute (prepared statements)
            return await self._conn.make_call(
                self._conn.protocol.execute,
                self._conn.protocol.build_response,
                params={
                    "statement_id": stmt,
                    "query_params": args,
                    "send_metadata": send_metadata,
                },
            )

    @online
    async def prepare(self, stmt: str) -> bytes:
        resp = await self._conn.make_call(
            self._conn.protocol.prepare,
            self._conn.protocol.build_response,
            params={"query": stmt},
        )
        assert isinstance(resp, bytes)
        return resp
