from asyncio import Queue  # noqa
from typing import Any, Dict, List, Optional, Union  # noqa

from .constants import SchemaChangeTarget, SchemaChangeType  # noqa

ExpectedResponses = Union[
    str, bool, "Rows", bytes, "SchemaChange", "Queue[Any]", Dict[str, List[str]]
]


class BaseType:
    pass


class UnknownType(BaseType):
    pass


class AsciiType(BaseType):
    pass


class ChangeEvent:
    pass


class SchemaChange(ChangeEvent):
    def __init__(
        self,
        change_type: "SchemaChangeType",
        target: "SchemaChangeTarget",
        options: dict,
    ) -> None:
        self.change_type = change_type
        self.target = target
        self.options = options


class Rows:
    def __init__(self, columns_count: int) -> None:
        self.index: int = 0
        self._data: List[Optional[bytes]] = []
        self.columns_count = columns_count

    def __iter__(self) -> "Rows":
        return self

    def add(self, cell: Optional[bytes]) -> None:
        self._data.append(cell)

    def __next__(self) -> List[Optional[bytes]]:
        if self.index == len(self._data):
            # reset
            self.index = 0
            raise StopIteration
        current = self._data[self.index : self.index + self.columns_count]
        self.index += self.columns_count
        return current
