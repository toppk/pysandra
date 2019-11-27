from asyncio import Queue  # noqa
from collections import OrderedDict
from ipaddress import IPv4Address, IPv6Address  # noqa: F401
from typing import (  # noqa
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from .constants import (  # noqa
    NodeStatus,
    SchemaChangeTarget,
    SchemaChangeType,
    TopologyStatus,
)
from .utils import get_logger

logger = get_logger(__name__)
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


class Row:
    def __init__(self, *args: Any, fields: List[str]) -> None:
        self.args = args
        self.fields = fields

    def __repr__(self) -> str:
        return self.__class__.__name__ + "(%s)" % ", ".join(
            [f"{k}={v!r}" for k, v in zip(self.fields, self.args)]
        )

    def __iter__(self) -> Iterator[Any]:
        return self.args.__iter__()

    def __getattr__(self, name: str) -> str:
        return self.args[self.fields.index(name)]

    def __getitem__(self, index: int) -> Optional[bytes]:
        return self.args[index]

    def _asdict(self) -> "OrderedDict":
        return OrderedDict(zip(self.fields, self.args))


class Rows:
    """
    This is the container for data queried from cassandra
    """

    def __init__(
        self, columns_count: int, col_specs: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        self.index: int = 0
        self._data: List[Union["Row", Tuple[Optional[bytes], ...]]] = []
        self.columns_count = columns_count
        self._col_specs: Optional[List[Dict[str, Any]]] = None
        self._fields: Optional[List[str]] = None
        if col_specs is not None:
            self.col_specs = col_specs

    def __iter__(self) -> "Rows":
        return self

    @property
    def col_specs(self) -> Optional[List[Dict[str, Any]]]:
        return self._col_specs

    @col_specs.setter
    def col_specs(self, col_specs: List[Dict[str, Any]]) -> None:
        self._col_specs = col_specs
        self._fields = [col["name"] for col in col_specs]
        if len(self._data) > 0:
            rows = []
            for row in self._data:
                rows.append(Row(*row, fields=self._fields))
            self._data.clear()
            for row in rows:
                self._data.append(row)

    def add_row(self, row: Tuple[Optional[bytes], ...]) -> None:
        if self._fields is not None:
            self._data.append(Row(*row, fields=self._fields))
        else:
            self._data.append(row)

    def __next__(self) -> Union["Row", Tuple[Optional[bytes], ...]]:
        if self.index == len(self._data):
            # reset
            self.index = 0
            raise StopIteration
        current = self._data[self.index]
        self.index += 1
        return current
