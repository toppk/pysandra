from enum import Enum
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, cast

from .exceptions import InternalDriverError, MaximumStreamsException
from .utils import get_logger

# from .protocol import RequestMessage  # noqa: F401
logger = get_logger(__name__)


class HexEnum(int, Enum):
    def __format__(self, format_spec: str) -> str:
        if format_spec == "r":
            return f"{self._value_}"
        return hex(self)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}.{self._name_}: {hex(self)}>"


def pretty_type(value: Any) -> str:
    if type(value) == str:
        return "str"
    elif type(value) == int:
        return "int"
    return str(type(value))


class SBytes(bytes):
    _index: int = 0

    def __new__(cls: Type[bytes], val: bytes) -> "SBytes":
        return cast(SBytes, super().__new__(cls, val))  # type: ignore # https://github.com/python/typeshed/issues/2630

    def hex(self) -> str:
        return "0x" + super().hex()

    def grab(self, count: int) -> bytes:
        assert self._index is not None
        if self._index + count > len(self):
            raise InternalDriverError(
                f"cannot go beyond {len(self)} count={count} index={self._index} sbytes={self!r}"
            )
        curindex = self._index
        self._index += count
        return self[curindex : curindex + count]

    def at_end(self) -> bool:
        return self._index == len(self)

    @property
    def remaining(self) -> bytes:
        return self[self._index :]


T = TypeVar("T")


class Streams(Generic[T]):
    def __init__(self) -> None:
        self._last_stream_id: Optional[int] = None
        self._streams: Dict[int, Optional[T]] = {}

    def items(self) -> List[int]:
        return list(self._streams.keys())

    def remove(self, stream_id: int) -> T:
        try:
            store = self._streams.pop(stream_id)
            assert store is not None
            return store
        except KeyError:
            raise InternalDriverError(
                f"stream_id={stream_id} is not open", stream_id=stream_id
            )

    def create(self) -> int:
        maxstream = 2 ** 15
        last_id = self._last_stream_id
        if len(self._streams) > maxstream:
            raise MaximumStreamsException(
                f"too many streams last_id={last_id} length={len(self._streams)}"
            )
        next_id = 0x00
        if last_id is not None:
            next_id = last_id + 1
            while True:
                if next_id > maxstream:
                    next_id = 0x00
                if next_id not in self._streams:
                    break
                next_id = next_id + 1
        # store will come in later
        self._streams[next_id] = None
        self._last_stream_id = next_id
        return next_id

    def update(self, stream_id: int, store: T) -> None:
        if stream_id not in self._streams:
            raise InternalDriverError(f"stream_id={stream_id} not being tracked")
        if store is None:
            raise InternalDriverError("cannot store empty request")
        self._streams[stream_id] = store
