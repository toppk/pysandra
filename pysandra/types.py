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
        self.ipaddr = ipaddr
        self.port = port

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InetType):
            return NotImplemented
        return (
            self.__class__ == other.__class__
            and self.port == other.port
            and self.ipaddr == other.ipaddr
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(ipaddr={self.ipaddr!r}, port={self.port})"


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
    def __init__(
        self,
        *args: "ExpectedType",
        fields_: Optional[List[str]] = None,
        **kwargs: "ExpectedType",
    ) -> None:
        if fields_ is not None and len(args) > 0:
            self._args = args
            self._fields = fields_
        else:
            self._args = tuple(kwargs.values())
            self._fields = list(kwargs.keys())

    def __repr__(self) -> str:
        return self.__class__.__name__ + "(%s)" % ", ".join(
            [f"{k}={v!r}" for k, v in zip(self._fields, self._args)]
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Row):
            return NotImplemented
        return (
            self.__class__ == other.__class__
            and self._args == other._args
            and self._fields == other._fields
        )

    def __len__(self) -> int:
        return len(self._args)

    def __iter__(self) -> Iterator[Any]:
        return self._args.__iter__()

    def __getattr__(self, name: str) -> "ExpectedType":
        try:
            return self._args[self._fields.index(name)]
        except ValueError:
            raise AttributeError

    def __dir__(self) -> List[str]:
        return [
            x for x in super().__dir__() if x not in ("_args", "_fields")
        ] + self._fields

    # return self.fields
    def __getitem__(self, index: int) -> "ExpectedType":
        return self._args[index]

    def asdict_(self) -> "OrderedDict":
        return OrderedDict(zip(self._fields, self._args))


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
                rows.append(Row(*row, fields_=self._fields))
            self._data.clear()
            for row in rows:
                self._data.append(row)

    def add_row(self, row: Tuple["ExpectedType", ...]) -> None:
        if self._fields is not None:
            self._data.append(Row(*row, fields_=self._fields))
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
