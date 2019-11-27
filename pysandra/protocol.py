from collections.abc import Collection
from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from struct import Struct, pack, unpack
from sys import byteorder
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from .constants import (
    COMPRESS_MINIMUM,
    SERVER_SENT,
    Consistency,
    ErrorCode,
    Events,
    Flags,
    Kind,
    NodeStatus,
    Opcode,
    OptionID,
    QueryFlags,
    ResultFlags,
    SchemaChangeTarget,
    TopologyStatus,
    WriteType,
)
from .core import SBytes, pretty_type
from .exceptions import (
    BadInputException,
    InternalDriverError,
    TypeViolation,
    UnknownPayloadException,
    VersionMismatchException,
)
from .types import (
    ChangeEvent,
    ExpectedResponses,
    InetType,
    Rows,
    SchemaChange,
    SchemaChangeType,
    StatusChange,
    TopologyChange,
)
from .utils import get_logger

logger = get_logger(__name__)

# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec

"""

    [int]          A 4 bytes integer
    [long]         A 8 bytes integer
    [short]        A 2 bytes unsigned integer
    [string]       A [short] n, followed by n bytes representing an UTF-8
                   string.
    [long string]  An [int] n, followed by n bytes representing an UTF-8 string.
    [uuid]         A 16 bytes long uuid.
    [string list]  A [short] n, followed by n [string].
    [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
                   no byte should follow and the value represented is `null`.
    [value]        A [int] n, followed by n bytes if n >= 0.
                   If n == -1 no byte should follow and the value represented is `null`.
                   If n == -2 no byte should follow and the value represented is
                   `not set` not resulting in any change to the existing value.
                   n < -2 is an invalid value and results in an error.
    [short bytes]  A [short] n, followed by n bytes if n >= 0.

    [option]       A pair of <id><value> where <id> is a [short] representing
                   the option id and <value> depends on that option (and can be
                   of size 0). The supported id (and the corresponding <value>)
                   will be described when this is used.
    [option list]  A [short] n, followed by n [option].
    [inet]         An address (ip and port) to a node. It consists of one
                   [byte] n, that represents the address size, followed by n
                   [byte] representing the IP address (in practice n can only be
                   either 4 (IPv4) or 16 (IPv6)), following by one [int]
                   representing the port.
    [consistency]  A consistency level specification. This is a [short]
                   representing a consistency level with the following
                   correspondance:
                     0x0000    ANY
                     0x0001    ONE
                     0x0002    TWO
                     0x0003    THREE
                     0x0004    QUORUM
                     0x0005    ALL
                     0x0006    LOCAL_QUORUM
                     0x0007    EACH_QUORUM
                     0x0008    SERIAL
                     0x0009    LOCAL_SERIAL
                     0x000A    LOCAL_ONE

    [string map]      A [short] n, followed by n pair <k><v> where <k> and <v>
                      are [string].
    [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
                      [string] and <v> is a [string list].
    [bytes map]       A [short] n, followed by n pair <k><v> where <k> is a
                      [string] and <v> is a [bytes].

"""

NETWORK_ORDER = "!"


class STypes(str, Enum):
    NetOrder = "!"
    INT = "l"
    LONG = "q"
    SHORT = "h"
    USHORT = "H"
    BYTE = "B"
    CHAR = "s"


structs: dict = {}


def get_struct(fmt: str) -> Struct:
    global structs
    if len(structs) == 0:
        formats = [
            f"{NETWORK_ORDER}{STypes.USHORT}",
            f"{NETWORK_ORDER}{STypes.INT}",
            f"{NETWORK_ORDER}{STypes.BYTE}",
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}",
            f"{NETWORK_ORDER}{STypes.USHORT}{STypes.BYTE}",
        ]
        for frmt in formats:
            structs[frmt] = Struct(frmt)
    if fmt not in structs:
        raise InternalDriverError(f"format={fmt} not cached")
    return structs[fmt]


# encoders


def encode_short(value: int) -> bytes:
    return get_struct(f"{NETWORK_ORDER}{STypes.USHORT}").pack(value)


def encode_int(value: int) -> bytes:
    return get_struct(f"{NETWORK_ORDER}{STypes.INT}").pack(value)


def encode_string(value: Union[str, bytes]) -> bytes:
    if isinstance(value, bytes):
        value_bytes = value
    else:
        value_bytes = value.encode("utf-8")
    return encode_short(len(value_bytes)) + pack(
        f"{NETWORK_ORDER}{len(value_bytes)}{STypes.CHAR}", value_bytes
    )


def encode_value(value: Optional[Union[str, bytes, int]]) -> bytes:
    if value is None:
        return encode_int(-1)
    if isinstance(value, int):
        value_bytes = encode_int(value)
    elif isinstance(value, str):
        value_bytes = value.encode("utf-8")
    elif isinstance(value, bytes):
        value_bytes = value
    return encode_int(len(value_bytes)) + pack(
        f"{NETWORK_ORDER}{len(value_bytes)}{STypes.CHAR}", value_bytes
    )


def encode_long_string(value: Union[str, bytes]) -> bytes:
    if isinstance(value, bytes):
        value_bytes = value
    else:
        value_bytes = value.encode("utf-8")
    return encode_int(len(value_bytes)) + pack(
        f"{NETWORK_ORDER}{len(value_bytes)}{STypes.CHAR}", value_bytes
    )


def encode_strings_list(values: List[str]) -> bytes:
    data = encode_short(len(values))
    for value in values:
        data += encode_string(value)
    return data


# decoders


def decode_short(sbytes: "SBytes") -> int:
    return unpack(f"{NETWORK_ORDER}{STypes.USHORT}", sbytes.grab(2))[0]


def decode_int(sbytes: "SBytes") -> int:
    return unpack(f"{NETWORK_ORDER}{STypes.INT}", sbytes.grab(4))[0]


def decode_short_bytes(sbytes: "SBytes") -> bytes:
    length = decode_short(sbytes)
    if length == 0:
        return b""
    return unpack(f"{NETWORK_ORDER}{length}{STypes.CHAR}", sbytes.grab(length))[0]


def decode_length_bytes(sbytes: "SBytes", length: int) -> bytes:
    assert length is not None
    if length == 0:
        return b""
    return unpack(f"{NETWORK_ORDER}{length}{STypes.CHAR}", sbytes.grab(length))[0]


def decode_int_bytes(sbytes: "SBytes") -> Optional[bytes]:
    length = decode_int(sbytes)
    if length == 0:
        return b""
    elif length < 0:
        return None
    return unpack(f"{NETWORK_ORDER}{length}{STypes.CHAR}", sbytes.grab(length))[0]


def decode_consistency(sbytes: "SBytes") -> "Consistency":
    code = decode_short(sbytes)
    try:
        return Consistency(code)
    except ValueError:
        raise InternalDriverError(f"unknown consistency={code:x}")


def decode_byte(sbytes: "SBytes") -> int:
    return get_struct(f"{NETWORK_ORDER}{STypes.BYTE}").unpack(sbytes.grab(1))[0]


def decode_inet(sbytes: "SBytes") -> "InetType":
    length = decode_byte(sbytes)
    if length not in (4, 16):
        raise InternalDriverError(f"unhandled inet length={length}")
    address = decode_length_bytes(sbytes, length)
    intaddress = int.from_bytes(address, byteorder=byteorder)
    ipaddress = IPv4Address(intaddress) if length == 4 else IPv6Address(intaddress)
    port = decode_int(sbytes)
    assert isinstance(ipaddress, IPv4Address) or isinstance(ipaddress, IPv6Address)
    return InetType(ipaddress, port)


def decode_string(sbytes: "SBytes") -> str:
    return decode_short_bytes(sbytes).decode("utf-8")


def decode_strings_list(sbytes: "SBytes") -> List[str]:
    string_list = []
    num_strings = decode_short(sbytes)
    for _cnt in range(num_strings):
        string_list.append(decode_string(sbytes))
    return string_list


def decode_string_multimap(sbytes: "SBytes") -> Dict[str, List[str]]:
    num_entries = decode_short(sbytes)
    multimap: Dict[str, List[str]] = {}
    for _cnt in range(num_entries):
        logger.debug(f"multimap num_entries={num_entries}")
        key = decode_string(sbytes)
        values = decode_strings_list(sbytes)
        multimap[key] = values
    logger.debug("end multimap")
    return multimap


# Header = namedtuple('Header', 'version flags stream_id opcode')


class Protocol:
    version: int
    compress: Optional[Callable]

    def __init__(self, server_role: bool = False) -> None:
        self.server_role = server_role

    def decode_header(self, header: bytes) -> Tuple[int, int, int, int, int]:
        version, flags, stream, opcode, length = get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).unpack(header)
        logger.debug(
            f"got head={header!r} containing version={version:x} flags={flags:x} stream={stream:x} opcode={opcode:x} length={length:x}"
        )
        if self.server_role:
            expected_version = ~SERVER_SENT & self.version
        else:
            expected_version = SERVER_SENT | self.version
        if version != expected_version:
            raise VersionMismatchException(
                f"received incorrect version from server, go version={hex(version)} expected version={hex(expected_version)}"
            )
        return version, flags, stream, opcode, length

    def event_handler(
        self,
        version: int,
        flags: int,
        stream_id: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> None:
        raise InternalDriverError("subclass should implement method")

    def build_response(
        self,
        request: "RequestMessage",
        version: int,
        flags: int,
        stream: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> "ExpectedResponses":
        raise InternalDriverError("subclass should implement method")

    def startup(self, stream_id: int, params: dict) -> "StartupMessage":
        raise InternalDriverError("subclass should implement method")

    def query(self, stream_id: int, params: dict) -> "QueryMessage":
        raise InternalDriverError("subclass should implement method")

    def execute(self, stream_id: int, params: dict) -> "ExecuteMessage":
        raise InternalDriverError("subclass should implement method")

    def prepare(self, stream_id: int, params: dict) -> "PrepareMessage":
        raise InternalDriverError("subclass should implement method")

    def options(self, stream_id: int, params: dict) -> "OptionsMessage":
        raise InternalDriverError("subclass should implement method")

    def register(self, stream_id: int, params: dict) -> "RegisterMessage":
        raise InternalDriverError("subclass should implement method")


class BaseMessage:
    pass


class RequestMessage(BaseMessage):
    opcode: int

    def __init__(
        self, version: int, flags: int, stream_id: int, compress: Callable = None
    ) -> None:
        self.version = version
        self.flags = flags
        self.stream_id = stream_id
        self.compress = compress

    def __bytes__(self) -> bytes:
        body: bytes = self.encode_body()
        if self.compress is not None and len(body) >= COMPRESS_MINIMUM:
            self.flags |= Flags.COMPRESSION
            logger.debug("compressing the request")
            body = self.compress(body)
        header: bytes = self.encode_header(len(body))
        logger.debug(
            f"encoded request opcode={self.opcode} header={header!r} body={body!r}"
        )
        return header + body

    def encode_body(self) -> bytes:
        return b""

    def encode_header(self, body_length: int) -> bytes:
        return get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).pack(self.version, self.flags, self.stream_id, self.opcode, body_length)


class ResponseMessage(BaseMessage):
    opcode: int = -1

    def __init__(self, version: int, flags: int, stream_id: int) -> None:
        self.version = version
        self.flags = flags
        self.stream_id = stream_id

    @classmethod
    def create(
        cls: Type["ResponseMessage"],
        version: int,
        flags: int,
        stream_id: int,
        body: "SBytes",
    ) -> "ResponseMessage":
        logger.debug(f"creating msg class={cls} with body={body!r}")
        msg = cls.build(version, flags, stream_id, body)
        if not body.at_end():
            raise InternalDriverError(
                f"class={cls}.build() left data remains={body.remaining!r}"
            )
        if msg is None:
            raise InternalDriverError(
                f"didn't generate a response message for opcode={cls.opcode}"
            )
        return msg

    @staticmethod
    def build(
        version: int, flags: int, stream_id: int, body: "SBytes"
    ) -> "ResponseMessage":
        raise InternalDriverError("subclass should implement method")


class ReadyMessage(ResponseMessage):
    opcode = Opcode.READY

    @staticmethod
    def build(
        version: int, flags: int, stream_id: int, body: "SBytes"
    ) -> "ReadyMessage":
        logger.debug(f"ReadyResponse body={body!r}")
        return ReadyMessage(version, flags, stream_id)


class SupportedMessage(ResponseMessage):
    opcode = Opcode.SUPPORTED

    def __init__(self, options: Dict[str, List[str]], *args: Any) -> None:
        super().__init__(*args)
        self.options: Dict[str, List[str]] = options

    @staticmethod
    def build(
        version: int, flags: int, stream_id: int, body: "SBytes"
    ) -> "SupportedMessage":
        options = decode_string_multimap(body)
        logger.debug(f"SupportedResponse options={options} body={body!r}")
        return SupportedMessage(options, version, flags, stream_id)


class ErrorMessage(ResponseMessage):
    opcode = Opcode.ERROR

    def __init__(
        self, error_code: "ErrorCode", error_text: str, details: dict, *args: Any
    ) -> None:
        super().__init__(*args)
        self.error_code = error_code
        self.error_text = error_text
        self.details = details

    @staticmethod
    def build(
        version: int, flags: int, stream_id: int, body: "SBytes"
    ) -> "ErrorMessage":
        logger.debug(f"ErrorResponse body={body!r}")
        details: dict = {}
        code = decode_int(body)
        try:
            error_code = ErrorCode(code)
        except ValueError:
            raise InternalDriverError(f"unknown error_code={code:x}")
        error_text = decode_string(body)
        if error_code == ErrorCode.UNAVAILABLE_EXCEPTION:
            #                 <cl><required><alive>
            details["consistency_level"] = decode_consistency(body)
            details["required"] = decode_int(body)
            details["alive"] = decode_int(body)
        elif error_code == ErrorCode.WRITE_TIMEOUT:
            #                 <cl><received><blockfor><writeType>
            details["consistency_level"] = decode_consistency(body)
            details["received"] = decode_int(body)
            details["block_for"] = decode_int(body)
            string = decode_string(body)
            try:
                details["write_type"] = WriteType(string)
            except ValueError:
                raise InternalDriverError("unknown write_type={string}")
        elif error_code == ErrorCode.READ_TIMEOUT:
            # <cl><received><blockfor><data_present>
            details["consistency_level"] = decode_consistency(body)
            details["received"] = decode_int(body)
            details["block_for"] = decode_int(body)
            details["data_present"] = decode_byte(body)
        elif error_code == ErrorCode.READ_FAILURE:
            # <cl><received><blockfor><numfailures><data_present>
            details["consistency_level"] = decode_consistency(body)
            details["received"] = decode_int(body)
            details["block_for"] = decode_int(body)
            details["num_failures"] = decode_int(body)
            details["data_present"] = decode_byte(body)
        elif error_code == ErrorCode.FUNCTION_FAILURE:
            details["keyspace"] = decode_string(body)
            details["function"] = decode_string(body)
            details["arg_types"] = decode_strings_list(body)
        elif error_code == ErrorCode.WRITE_FAILURE:
            # <cl><received><blockfor><numfailures><write_type>
            details["consistency_level"] = decode_consistency(body)
            details["received"] = decode_int(body)
            details["block_for"] = decode_int(body)
            details["num_failures"] = decode_int(body)
            details["data_present"] = decode_byte(body)
            string = decode_string(body)
            try:
                details["write_type"] = WriteType(string)
            except ValueError:
                raise InternalDriverError("unknown write_type={string}")
        elif error_code == ErrorCode.ALREADY_EXISTS:
            details["keyspace"] = decode_string(body)
            details["table"] = decode_string(body)
        elif error_code == ErrorCode.UNPREPARED:
            details["statement_id"] = decode_short_bytes(body)
        logger.debug(f"ErrorMessage error_code={error_code:x}")
        return ErrorMessage(error_code, error_text, details, version, flags, stream_id)


class EventMessage(ResponseMessage):
    opcode = Opcode.RESULT

    def __init__(self, event_type: "Events", event: "ChangeEvent", *args: Any) -> None:
        super().__init__(*args)
        self.event_type = event_type
        self.event = event

    @staticmethod
    def decode_schema_change(body: "SBytes") -> "SchemaChange":
        # <change_type>
        string = decode_string(body)
        try:
            change_type = SchemaChangeType(string)
        except ValueError:
            raise UnknownPayloadException(f"got unexpected change_type={string}")
        # <target>
        string = decode_string(body)
        try:
            target = SchemaChangeTarget(string)
        except ValueError:
            raise UnknownPayloadException(f"got unexpected target={string}")
        # <options>
        options: Dict[str, Union[str, List[str]]] = {}
        if target == SchemaChangeTarget.KEYSPACE:
            options["target_name"] = decode_string(body)
        elif target in (SchemaChangeTarget.TABLE, SchemaChangeTarget.TYPE):
            options["keyspace_name"] = decode_string(body)
            options["target_name"] = decode_string(body)
        elif target in (SchemaChangeTarget.FUNCTION, SchemaChangeTarget.AGGREGATE):
            options["keyspace_name"] = decode_string(body)
            options["target_name"] = decode_string(body)
            options["argument_types"] = decode_strings_list(body)

        logger.debug(
            f"SCHEMA_CHANGE change_type={change_type} target={target} options={options}"
        )
        return SchemaChange(change_type, target, options)

    @staticmethod
    def build(
        version: int, flags: int, stream_id: int, body: "SBytes",
    ) -> "EventMessage":
        event = decode_string(body)
        try:
            event_type = Events(event)
        except ValueError:
            raise UnknownPayloadException(f"got unexpected event={event}")
        event_obj: Optional["ChangeEvent"] = None
        if event_type == Events.TOPOLOGY_CHANGE:
            status = decode_string(body)
            try:
                topology_status = TopologyStatus(status)
            except ValueError:
                raise UnknownPayloadException(f"got unexpected status_change={status}")
            node = decode_inet(body)
            event_obj = TopologyChange(topology_status, node)
        elif event_type == Events.STATUS_CHANGE:
            status = decode_string(body)
            try:
                node_status = NodeStatus(status)
            except ValueError:
                raise UnknownPayloadException(f"got unexpected status_change={status}")
            node = decode_inet(body)
            event_obj = StatusChange(node_status, node)
        elif event_type == Events.SCHEMA_CHANGE:
            event_obj = EventMessage.decode_schema_change(body)
        assert event_obj is not None

        return EventMessage(event_type, event_obj, version, flags, stream_id)


class ResultMessage(ResponseMessage):
    opcode = Opcode.RESULT

    def __init__(self, kind: int, *args: Any) -> None:
        super().__init__(*args)
        self.kind = kind

    @staticmethod
    def decode_col_specs(
        result_flags: int, columns_count: int, body: "SBytes"
    ) -> List[dict]:
        # <global_table_spec>
        global_keyspace: Optional[str] = None
        global_table: Optional[str] = None
        if result_flags & ResultFlags.GLOBAL_TABLES_SPEC != 0:
            # <keyspace>
            global_keyspace = decode_string(body)
            # <table>
            global_table = decode_string(body)
            logger.debug(
                f"using global_table_spec keyspace={global_keyspace} table={global_table} result_flags={result_flags!r}"
            )
        # <col_spec_i>
        col_specs = []
        if columns_count > 0:
            for _col in range(columns_count):
                col_spec: Dict[str, Union[str, int]] = {}
                if result_flags & ResultFlags.GLOBAL_TABLES_SPEC == 0:
                    # <ksname><tablename>
                    logger.debug(f"not using global_table_spec")

                    col_spec["ksname"] = decode_string(body)
                    col_spec["tablename"] = decode_string(body)
                else:
                    assert global_keyspace is not None
                    assert global_table is not None
                    col_spec["ksname"] = global_keyspace
                    col_spec["tablename"] = global_table
                # <name><type>
                col_spec["name"] = decode_string(body)
                # <type>
                option_id = decode_short(body)
                if option_id < 0x0001 or option_id > 0x0014:
                    raise InternalDriverError(f"unhandled option_id={option_id}")
                col_spec["option_id"] = option_id
                col_specs.append(col_spec)
        logger.debug(f"col_specs={col_specs}")
        return col_specs

    @staticmethod
    def build(
        version: int, flags: int, stream_id: int, body: "SBytes",
    ) -> "ResultMessage":
        msg: Optional["ResultMessage"] = None
        kind = decode_int(body)
        logger.debug(f"ResultResponse kind={kind} body={body!r}")
        if kind == Kind.VOID:
            msg = VoidResultMessage(kind, version, flags, stream_id)
        elif kind == Kind.ROWS:
            result_flags = decode_int(body)
            columns_count = decode_int(body)
            logger.debug(
                f"ResultResponse result_flags={result_flags} columns_count={columns_count}"
            )
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                # parse paging state
                raise InternalDriverError(f"need to parse paging state")
            col_specs = None
            if (result_flags & ResultFlags.NO_METADATA) == 0x00 and columns_count > 0:
                col_specs = ResultMessage.decode_col_specs(
                    result_flags, columns_count, body
                )
            # parse rows
            rows = Rows(columns_count, col_specs=col_specs)
            rows_count = decode_int(body)
            for _rowcnt in range(rows_count):
                row: List[Optional[bytes]] = []
                for _colcnt in range(columns_count):
                    row.append(decode_int_bytes(body))
                rows.add_row(tuple(row))
            logger.debug(f"got col_specs={col_specs}")
            msg = RowsResultMessage(rows, kind, version, flags, stream_id)

        elif kind == Kind.SET_KEYSPACE:
            keyspace = decode_string(body)
            msg = SetKeyspaceResultMessage(keyspace, kind, version, flags, stream_id)
        elif kind == Kind.PREPARED:
            # <id>
            statement_id = decode_short_bytes(body)
            if statement_id == b"":
                raise InternalDriverError("cannot use empty prepared statement_id")
            # <metadata>
            # <flags>
            result_flags = decode_int(body)
            # <columns_count>
            columns_count = decode_int(body)
            # <pk_count>
            pk_count = decode_int(body)
            pk_index: List[int] = []
            if pk_count > 0:
                pk_index = list(
                    unpack(
                        f"{NETWORK_ORDER}{STypes.USHORT * pk_count}",
                        body.grab(2 * pk_count),
                    )
                )
            logger.debug(
                f"build statement_id={statement_id!r} result_flags={result_flags} "
                + f"columns_count={columns_count} pk_count={pk_count} pk_index={pk_index}"
            )
            col_specs = ResultMessage.decode_col_specs(
                result_flags, columns_count, body
            )
            # <result_metadata>
            # <flags>
            results_result_flags = decode_int(body)
            # <columns_count>
            results_columns_count = decode_int(body)
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                raise InternalDriverError(f"need to parse paging state")

            if bool(results_result_flags & ResultFlags.NO_METADATA) != bool(
                results_columns_count == 0
            ):
                raise InternalDriverError(
                    f" unexpected results_result_flags={results_result_flags} results_columns_count={results_columns_count}"
                )
            results_col_specs = None
            if (
                results_result_flags & ResultFlags.NO_METADATA == 0x00
                and results_columns_count > 0
            ):
                results_col_specs = ResultMessage.decode_col_specs(
                    results_result_flags, results_columns_count, body
                )
            msg = PreparedResultMessage(
                statement_id,
                col_specs,
                pk_index,
                kind,
                version,
                flags,
                stream_id,
                results_col_specs=results_col_specs,
            )
        elif kind == Kind.SCHEMA_CHANGE:
            schema_change = EventMessage.decode_schema_change(body)
            msg = SchemaResultMessage(schema_change, kind, version, flags, stream_id)
        else:
            raise UnknownPayloadException(f"RESULT message has unknown kind={kind}")
        if msg is None:
            raise InternalDriverError(
                f"ResultResponse no msg generated for body={body!r}"
            )
        return msg


class SetKeyspaceResultMessage(ResultMessage):
    def __init__(self, keyspace: str, *args: Any) -> None:
        self.keyspace: str = keyspace
        super().__init__(*args)


class RowsResultMessage(ResultMessage):
    def __init__(self, rows: "Rows", *args: Any) -> None:
        self.rows: "Rows" = rows
        super().__init__(*args)


class VoidResultMessage(ResultMessage):
    pass


class SchemaResultMessage(ResultMessage):
    def __init__(self, schema_change: "SchemaChange", *args: Any) -> None:
        self.schema_change: "SchemaChange" = schema_change
        super().__init__(*args)


class PreparedResultMessage(ResultMessage):
    query_id: bytes

    def __init__(
        self,
        statement_id: bytes,
        col_specs: List[Dict[str, Any]],
        pk_index: List[int],
        *args: Any,
        results_col_specs: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        super().__init__(*args)
        self.pk_index = pk_index
        self.col_specs = col_specs
        self.results_col_specs: Optional[List[Dict[str, Any]]] = results_col_specs
        self.statement_id = statement_id


class StartupMessage(RequestMessage):
    opcode = Opcode.STARTUP

    def __init__(self, options: Dict[str, str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.options = options
        assert self.compress is None

    def encode_body(self) -> bytes:
        body = get_struct(f"{NETWORK_ORDER}{STypes.USHORT}").pack(len(self.options))
        for key, value in self.options.items():
            body += encode_string(key) + encode_string(value)
        return body


class OptionsMessage(RequestMessage):
    opcode = Opcode.OPTIONS

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        assert self.compress is None


class ExecuteMessage(RequestMessage):
    opcode = Opcode.EXECUTE

    def __init__(
        self,
        query_id: bytes,
        query_params: Optional["Collection"],
        send_metadata: bool,
        col_specs: List[dict],
        *args: Any,
        consistency: "Consistency" = None,
        **kwargs: Any,
    ) -> None:
        self.consistency = consistency
        self.query_id = query_id
        if query_params is None:
            query_params = []
        self.query_params = query_params
        self.send_metadata = send_metadata
        self.col_specs = col_specs
        super().__init__(*args, **kwargs)

    def encode_body(self) -> bytes:
        # <id>
        body = encode_string(self.query_id)
        #   <query_parameters>
        #     <consistency><flags>
        # data check
        if self.col_specs is not None and len(self.col_specs) != len(self.query_params):
            raise BadInputException(
                f" count of execute params={len(self.query_params)} doesn't match prepared statement count={len(self.col_specs)}"
            )
        body += QueryMessage.encode_query_parameters(
            self.query_params,
            self.send_metadata,
            col_specs=self.col_specs,
            consistency=self.consistency,
        )

        #     [<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
        # <n>
        return body


class QueryMessage(RequestMessage):
    opcode = Opcode.QUERY

    def __init__(
        self,
        query: str,
        query_params: Optional["Collection"],
        send_metadata: bool,
        *args: Any,
        consistency: "Consistency" = None,
        **kwargs: Any,
    ) -> None:
        self.consistency = consistency
        self.query = query
        if query_params is None:
            query_params = []
        self.query_params: "Collection" = query_params
        self.send_metadata = send_metadata
        super().__init__(*args, **kwargs)

    # used by ExecuteMessage and QueryMessage
    @staticmethod
    def encode_query_parameters(
        query_params: "Collection",
        send_metadata: bool,
        col_specs: Optional[List[dict]] = None,
        consistency: "Consistency" = None,
    ) -> bytes:
        if consistency is None:
            consistency = Consistency.ONE

        body: bytes = b""
        #   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>][<keyspace>][<now_in_seconds>]
        flags: int = 0x00
        if len(query_params) > 0:
            flags |= QueryFlags.VALUES
            if isinstance(query_params, dict):
                flags |= QueryFlags.WITH_NAMES_FOR_VALUES
        if not send_metadata:
            flags |= QueryFlags.SKIP_METADATA
        body += get_struct(f"{NETWORK_ORDER}{STypes.USHORT}{STypes.BYTE}").pack(
            consistency, flags
        )
        if len(query_params) > 0:
            body += encode_short(len(query_params))
            if col_specs is not None:
                if isinstance(query_params, dict):
                    raise InternalDriverError(
                        "query_params with bind parameters not supported for prepared statement"
                    )

                for value, spec in zip(query_params, col_specs):
                    if spec["option_id"] == OptionID.INT:
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.INT}", 4, value
                        )
                    elif spec["option_id"] == OptionID.VARCHAR:
                        if not isinstance(value, str):
                            raise BadInputException(
                                f"expected type=str but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += encode_long_string(value)
                    else:
                        raise InternalDriverError(
                            f"cannot handle unknown option_id={spec['option_id']}"
                        )
            else:
                if isinstance(query_params, dict):
                    # [name_n]<value_n>
                    for key, value in query_params.items():
                        body += encode_string(key) + encode_value(value)
                else:
                    # <value_n>
                    for value in query_params:
                        body += encode_value(value)
        logger.debug(
            f"lets' see the body={body!r} query_params={query_params} flags={flags}"
        )
        return body

    def encode_body(self) -> bytes:
        body = encode_long_string(self.query)
        body += QueryMessage.encode_query_parameters(
            self.query_params, self.send_metadata, consistency=self.consistency
        )
        return body


class RegisterMessage(RequestMessage):
    opcode = Opcode.REGISTER

    def __init__(self, events: List[Events], *args: Any, **kwargs: Any) -> None:
        self.events = events
        super().__init__(*args, **kwargs)

    def encode_body(self) -> bytes:
        checked: List[str] = []
        for event in self.events:
            try:
                checked.append(Events(event))
            except ValueError:
                raise TypeViolation(
                    f"got unknown event={event}. please use pysandra.Events"
                )
        return encode_strings_list(checked)


class PrepareMessage(RequestMessage):
    opcode = Opcode.PREPARE

    def __init__(self, query: str, *args: Any, **kwargs: Any) -> None:
        self.query = query
        super().__init__(*args, **kwargs)

    def encode_body(self) -> bytes:
        return encode_long_string(self.query)
