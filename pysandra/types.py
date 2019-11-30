import datetime  # noqa: F401
import decimal  # noqa: F401
import ipaddress  # noqa: F401
import uuid  # noqa: F401
from asyncio import Queue  # noqa
from collections import OrderedDict
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

ExpectedType = Optional[
    Union[
        bytes,
        str,
        int,
        float,
        bool,
        "uuid.UUID",
        "decimal.Decimal",
        "ipaddress.IPv4Address",
        "ipaddress.IPv6Address",
        "datetime.date",
        "datetime.datetime",
    ]
]


class BaseType:
    pass


class UnknownType(BaseType):
    pass


class AsciiType(BaseType):
    pass


class InetType(BaseType):
    def __init__(
        self,
        ipaddr: Union["ipaddress.IPv4Address", "ipaddress.IPv6Address"],
        port: int,
    ) -> None:
        self.ipaddr = ipaddress
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
    def __init__(self, *args: "ExpectedType", fields: List[str]) -> None:
        self.args = args
        self.fields = fields

    def __repr__(self) -> str:
        return self.__class__.__name__ + "(%s)" % ", ".join(
            [f"{k}={v!r}" for k, v in zip(self.fields, self.args)]
        )

    def __len__(self) -> int:
        return len(self.args)

    def __iter__(self) -> Iterator[Any]:
        return self.args.__iter__()

    def __getattr__(self, name: str) -> "ExpectedType":
        return self.args[self.fields.index(name)]

    def __dir__(self) -> List[str]:
        return [
            x for x in super().__dir__() if x not in ("args", "fields")
        ] + self.fields

    # return self.fields
    def __getitem__(self, index: int) -> "ExpectedType":
        return self.args[index]

    def asdict_(self) -> "OrderedDict":
        return OrderedDict(zip(self.fields, self.args))


class Rows:
    """
    This is the container for data queried from cassandra
    """

    def __init__(
        self, columns_count: int, col_specs: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        self.index: int = 0
        self._data: List[Union["Row", Tuple["ExpectedType", ...]]] = []
        self.columns_count = columns_count
        self._col_specs: Optional[List[Dict[str, Any]]] = None
        self._fields: Optional[List[str]] = None
        if col_specs is not None:
            self.col_specs = col_specs

    def __iter__(self) -> "Rows":
        return self

    def __getitem__(self, index: int) -> Union["Row", Tuple["ExpectedType", ...]]:
        return self._data[index]

    def __len__(self) -> int:
        return len(self._data)

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

    def add_row(self, row: Tuple["ExpectedType", ...]) -> None:
        if self._fields is not None:
            self._data.append(Row(*row, fields=self._fields))
        else:
            self._data.append(row)

    def __next__(self) -> Union["Row", Tuple["ExpectedType", ...]]:
        if self.index == len(self._data):
            # reset
            self.index = 0
            raise StopIteration
        current = self._data[self.index]
        self.index += 1
        return current
