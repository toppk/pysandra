from asyncio import Queue  # noqa
from ipaddress import IPv4Address, IPv6Address  # noqa: F401
from typing import Any, Dict, List, Optional, Union  # noqa

from .constants import (  # noqa
    NodeStatus,
    SchemaChangeTarget,
    SchemaChangeType,
    TopologyStatus,
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


class InetType(BaseType):
    def __init__(
        self, ipaddress: Union["IPv4Address", "IPv6Address"], port: int
    ) -> None:
        self.ipaddress = ipaddress
        self.port = port


class ChangeEvent:
    pass


class TopologyChange(ChangeEvent):
    def __init__(self, topology_status: "TopologyStatus", node: "InetType") -> None:
        self.topology_status = topology_status
        self.node = node


class StatusChange(ChangeEvent):
    def __init__(self, node_status: "NodeStatus", node: "InetType") -> None:
        self.node_status = node_status
        self.node = node


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
        self.col_specs: Optional[List[Dict[str, Any]]] = None

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
