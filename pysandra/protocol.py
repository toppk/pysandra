from enum import Enum
from struct import Struct, pack, unpack
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .constants import (
    COMPRESS_MINIMUM,
    SERVER_SENT,
    Consitency,
    ErrorCode,
    Events,
    Flags,
    Kind,
    Opcode,
    OptionID,
    QueryFlags,
    ResultFlags,
    SchemaChangeTarget,
)
from .exceptions import (
    BadInputException,
    InternalDriverError,
    TypeViolation,
    UnknownPayloadException,
    VersionMismatchException,
)
from .types import ChangeEvent, ExpectedResponses, Rows, SchemaChange, SchemaChangeType
from .utils import SBytes, get_logger

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


def decode_int_bytes(sbytes: "SBytes") -> Optional[bytes]:
    length = decode_int(sbytes)
    if length == 0:
        return b""
    elif length < 0:
        return None
    return unpack(f"{NETWORK_ORDER}{length}{STypes.CHAR}", sbytes.grab(length))[0]


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
        key = decode_string(sbytes)
        values = decode_strings_list(sbytes)
        multimap[key] = values
    return multimap


class Protocol:
    version: int
    compress: Optional[Callable]

    def decode_header(self, header: bytes) -> Tuple[int, int, int, int, int]:
        version, flags, stream, opcode, length = get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).unpack(header)
        logger.debug(
            f"got head={header!r} containing version={version:x} flags={flags:x} stream={stream:x} opcode={opcode:x} length={length:x}"
        )
        expected_version = SERVER_SENT | self.version
        if version != expected_version:
            raise VersionMismatchException(
                f"received version={version:x} instead of expected_version={expected_version}"
            )
        return version, flags, stream, opcode, length

    async def event_handler(
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
        if self.compress is not None and len(body) > COMPRESS_MINIMUM:
            self.flags |= Flags.COMPRESSION
            logger.debug("compressing the request")
            body = self.compress(body)
        header: bytes = self.encode_header(len(body))
        logger.debug(f"opcode={self.opcode} header={header!r} body={body!r}")
        return header + body

    def encode_body(self) -> bytes:
        return b""

    def encode_header(self, body_length: int) -> bytes:
        return get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).pack(self.version, self.flags, self.stream_id, self.opcode, body_length)


class ResponseMessage(BaseMessage):
    opcode: int

    def __init__(self, version: int, flags: int, stream_id: int) -> None:
        self.version = version
        self.flags = flags
        self.stream_id = stream_id

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
            raise InternalDriverError(f"unknown error code={code}")
        error_text = decode_string(body)
        if error_code == ErrorCode.UNAVAILABLE_EXCEPTION:
            pass
        elif error_code == ErrorCode.WRITE_TIMEOUT:
            pass
        elif error_code == ErrorCode.READ_TIMEOUT:
            pass
        elif error_code == ErrorCode.READ_FAILURE:
            pass
        elif error_code == ErrorCode.FUNCTION_FAILURE:
            details["keyspace"] = decode_string(body)
            details["function"] = decode_string(body)
            details["arg_types"] = decode_strings_list(body)
        elif error_code == ErrorCode.WRITE_FAILURE:
            pass
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
        try:
            string = decode_string(body)
            change_type = SchemaChangeType(string)
        except ValueError:
            raise UnknownPayloadException(f"got unexpected change_type={change_type}")
        # <target>
        try:
            string = decode_string(body)
            target = SchemaChangeTarget(string)
        except ValueError:
            raise UnknownPayloadException(f"got unexpected target={target}")
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
        else:
            raise InternalDriverError(
                f"unhandled target={target} with body={body.remaining!r}"
            )

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
        if event_type == Events.TOPOLOGY_CHANGE:
            pass
        elif event_type == Events.STATUS_CHANGE:
            pass
        elif event_type == Events.SCHEMA_CHANGE:
            event_obj = EventMessage.decode_schema_change(body)

        if event_obj is None:
            raise InternalDriverError(
                "didn't create and Event for event_type={event_type}"
            )

        logger.debug(f"EventResponse body={body!r}")

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
                f"build keyspace={global_keyspace} table={global_table} result_flags={result_flags!r}"
            )
        # <col_spec_i>
        col_specs = []
        if columns_count > 0:
            for _col in range(columns_count):
                col_spec: Dict[str, Union[str, int]] = {}
                if result_flags & ResultFlags.GLOBAL_TABLES_SPEC == 0:
                    # <ksname><tablename>
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
            column_count = decode_int(body)
            logger.debug(
                f"ResultResponse result_flags={result_flags} column_count={column_count}"
            )
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                # parse paging state
                raise InternalDriverError(f"need to parse paging state")
            col_specs: Optional[List[Dict[str, Any]]] = None
            if result_flags & ResultFlags.NO_METADATA == 0x00:
                col_specs = ResultMessage.decode_col_specs(
                    result_flags, column_count, body
                )
            # parse rows
            rows = Rows(column_count=column_count)
            rows_count = decode_int(body)
            for _cnt in range(rows_count * column_count):
                rows.add(decode_int_bytes(body))
            msg = RowsResultMessage(
                rows, kind, version, flags, stream_id, col_specs=col_specs
            )

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
            pk_index = None
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
            result_flags = decode_int(body)
            # <columns_count>
            result_columns_count = decode_int(body)
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                raise InternalDriverError(f"need to parse paging state")
            if result_flags & ResultFlags.NO_METADATA == 0x00:
                if result_flags & ResultFlags.GLOBAL_TABLES_SPEC != 0x00:
                    # parse global_table_spec
                    pass
                # parse col_spec_i
                for _col in range(result_columns_count):
                    pass
            msg = PreparedResultMessage(
                statement_id, col_specs, kind, version, flags, stream_id
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
    def __init__(
        self, rows: "Rows", *args: Any, col_specs: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        self.rows: "Rows" = rows
        self.col_specs: Optional[List[Dict[str, Any]]] = col_specs
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
        self, statement_id: bytes, col_specs: List[Dict[str, Any]], *args: Any
    ) -> None:
        super().__init__(*args)
        self.col_specs = col_specs
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
        query_params: dict,
        send_metadata: bool,
        col_specs: List[dict],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.query_id = query_id
        self.query_params = query_params
        self.send_metadata = send_metadata
        self.col_specs = col_specs
        super().__init__(*args, **kwargs)

    def encode_body(self) -> bytes:
        # data check
        if len(self.col_specs) != len(self.query_params):
            raise BadInputException(
                f" count of execute params={len(self.query_params)} doesn't match prepared statement count={len(self.col_specs)}"
            )
        # <id>
        body = encode_string(self.query_id)
        #   <query_parameters>
        #     <consistency><flags>
        flags: int = QueryFlags.VALUES
        if not self.send_metadata:
            flags |= QueryFlags.SKIP_METADATA
        body += get_struct(f"{NETWORK_ORDER}{STypes.USHORT}{STypes.BYTE}").pack(
            Consitency.ONE, QueryFlags.VALUES
        )
        #     [<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
        # <n>
        body += get_struct(f"{NETWORK_ORDER}{STypes.USHORT}").pack(len(self.col_specs))
        for value, spec in zip(self.query_params, self.col_specs):
            if spec["option_id"] == OptionID.INT:
                if not isinstance(value, int):
                    raise BadInputException(
                        f"expected int but got type={type(value)} for value={value!r}"
                    )
                body += pack(f"{NETWORK_ORDER}{STypes.INT}{STypes.INT}", 4, value)
            elif spec["option_id"] == OptionID.VARCHAR:
                if not isinstance(value, str):
                    raise BadInputException(
                        f"expected str but got type={type(value)} for value={value!r}"
                    )
                body += encode_long_string(value)
            else:
                raise InternalDriverError(
                    f"cannot handle unknown option_id={spec['option_id']}"
                )
        return body


class QueryMessage(RequestMessage):
    opcode = Opcode.QUERY

    def __init__(
        self, query: str, send_metadata: bool, *args: Any, **kwargs: Any
    ) -> None:
        self.query = query
        self.send_metadata = send_metadata
        super().__init__(*args, **kwargs)

    def encode_body(self) -> bytes:
        body = encode_long_string(self.query)
        flags = 0x00
        if not self.send_metadata:
            flags |= QueryFlags.SKIP_METADATA
        #   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>][<keyspace>][<now_in_seconds>]
        body += get_struct(f"{NETWORK_ORDER}{STypes.USHORT}{STypes.BYTE}").pack(
            Consitency.ONE, flags
        )
        # query_body += pack("!HL", Consitency.ONE, 0x0002)
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
