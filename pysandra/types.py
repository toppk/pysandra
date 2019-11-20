from asyncio import Queue  # noqa
from typing import Any, Dict, List, Optional, Union  # noqa

from .constants import (
    CQL_VERSION,
    DEFAULT_HOST,
    DEFAULT_PORT,
    Options,
    SchemaChangeTarget,
    SchemaChangeType,
)

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
    def __init__(self, column_count: int) -> None:
        self.index: int = 0
        self._data: List[Optional[bytes]] = []
        self.column_count = column_count

    def __iter__(self) -> "Rows":
        return self

    def add(self, cell: Optional[bytes]) -> None:
        self._data.append(cell)

    def __next__(self) -> List[Optional[bytes]]:
        if self.index == len(self._data):
            # reset
            self.index = 0
            raise StopIteration
        current = self._data[self.index : self.index + self.column_count]
        self.index += self.column_count
        return current


if __name__ == "__main__":
    d = Rows(column_count=2)
    d.add(b"1")
    d.add(b"2")
    d.add(b"3")
    d.add(b"4")
    for row in d:
        print(f"got row={row}")
    for row in d:
        print(f"got row={row}")


class Connection:
    def __init__(
        self,
        host: str = None,
        port: int = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if options is None:
            options = {Options.CQL_VERSION: CQL_VERSION}
        if host is None:
            host = DEFAULT_HOST
        if port is None:
            port = DEFAULT_PORT
        self.host = host
        self.port = port
        self._options = options
        self.supported_options: Optional[Dict[str, List[str]]] = None

    @property
    def options(self) -> Dict[str, str]:
        if self.supported_options is not None:
            # check options
            pass
        return self._options
