import datetime
import decimal
import ipaddress
import uuid
from collections.abc import Collection
from struct import pack, unpack
from typing import Any, Callable, Dict, List, Optional, Type, Union

from .codecs import (
    NETWORK_ORDER,
    STypes,
    decode_byte,
    decode_consistency,
    decode_inet,
    decode_int,
    decode_int_bytes_must,
    decode_short,
    decode_short_bytes,
    decode_string,
    decode_string_multimap,
    decode_strings_list,
    encode_bytes,
    encode_int,
    encode_long_string,
    encode_short,
    encode_string,
    encode_strings_list,
    encode_value,
    encode_varint,
    get_struct,
)
from .constants import (
    COMPRESS_MINIMUM,
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
)
from .types import ExpectedType  # noqa: F401
from .types import (
    ChangeEvent,
    Rows,
    SchemaChange,
    SchemaChangeType,
    StatusChange,
    TopologyChange,
)
from .utils import get_logger

logger = get_logger(__name__)


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
                row: List[Optional["ExpectedType"]] = []
                for colcnt in range(columns_count):
                    cell: Optional["ExpectedType"] = None
                    value = SBytes(decode_int_bytes_must(body))
                    if col_specs is None:
                        row.append(bytes(value))
                    else:
                        spec = col_specs[colcnt]
                        if spec["option_id"] in (
                            OptionID.INT,
                            OptionID.BIGINT,
                            OptionID.SMALLINT,
                            OptionID.TIME,
                            OptionID.TINYINT,
                            OptionID.VARINT,
                        ):
                            cell = int.from_bytes(value, byteorder="big", signed=True)
                        elif spec["option_id"] in (OptionID.BLOB,):
                            cell = value
                        elif spec["option_id"] in (OptionID.ASCII, OptionID.VARCHAR):
                            cell = value.decode("utf-8")
                        elif spec["option_id"] in (OptionID.DECIMAL,):
                            scale = decode_int(value)
                            unscaled = int.from_bytes(
                                value.remaining, byteorder="big", signed=True
                            )
                            cell = decimal.Decimal(unscaled) * 10 ** (
                                -1 * decimal.Decimal(scale)
                            )
                        elif spec["option_id"] in (OptionID.DOUBLE,):
                            cell = unpack(f"{NETWORK_ORDER}{STypes.DOUBLE}", value)[0]
                        elif spec["option_id"] in (OptionID.FLOAT,):
                            cell = unpack(f"{NETWORK_ORDER}{STypes.FLOAT}", value)[0]
                        elif spec["option_id"] in (OptionID.INET,):
                            ipaddr = int.from_bytes(
                                value.remaining, byteorder="big", signed=False
                            )
                            if len(value) not in (4, 16,):
                                raise InternalDriverError(
                                    f"option={spec['option_id']:x} not exepected length 4 or 16, length={len(value)}"
                                )
                            cell = (
                                ipaddress.IPv4Address(ipaddr)
                                if len(value) == 4
                                else ipaddress.IPv6Address(ipaddr)
                            )
                        elif spec["option_id"] in (OptionID.TIMEUUID, OptionID.UUID):
                            uuidint = int.from_bytes(
                                value.remaining, byteorder="big", signed=False
                            )
                            cell = uuid.UUID(int=uuidint)
                        elif spec["option_id"] in (OptionID.BOOLEAN,):
                            cell = False if value == b"\x00" else True
                        elif spec["option_id"] in (OptionID.TIMESTAMP,):
                            timestamp = int.from_bytes(
                                value, byteorder="big", signed=False
                            )
                            # logger.debug(f"date={date} pathma={date-2**31} value={value!r}")
                            cell = datetime.datetime.utcfromtimestamp(
                                timestamp / 10 ** 3
                            )
                        elif spec["option_id"] in (OptionID.DATE,):
                            date = int.from_bytes(value, byteorder="big", signed=False)
                            # logger.debug(f"date={date} pathma={date-2**31} value={value!r}")
                            cell = datetime.datetime.fromtimestamp(
                                0, datetime.timezone.utc
                            ).date() + datetime.timedelta(days=date - 2 ** 31)
                        else:
                            raise InternalDriverError(
                                f"unknown option_id={spec['option_id']:x} with value={value!r}"
                            )
                        # logger.debug(f"got cell={cell} with option_id={spec['option_id']:x} for value={value} with type={type(cell)}")
                        row.append(cell)

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
                    if spec["option_id"] in (OptionID.INT,):
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.INT}", 4, value
                        )
                    elif spec["option_id"] in (OptionID.TINYINT,):
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.BYTE}", 1, value
                        )
                    elif spec["option_id"] in (OptionID.SMALLINT,):
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.SHORT}", 2, value
                        )
                    elif spec["option_id"] in (OptionID.BIGINT,):
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.LONG}", 8, value
                        )
                    elif spec["option_id"] in (OptionID.VARINT,):
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        res = encode_varint(value)
                        body += encode_int(len(res)) + res
                    elif spec["option_id"] in (OptionID.BLOB,):
                        # what about buffer
                        if not isinstance(value, bytes) and not isinstance(
                            value, bytearray
                        ):
                            raise BadInputException(
                                f"expected type=bytes/bytearray but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += encode_bytes(value)
                    elif spec["option_id"] in (OptionID.TIME,):
                        # what about buffer
                        if not isinstance(value, int):
                            raise BadInputException(
                                f"expected type=int but got type={pretty_type(value)} for value={value!r}"
                            )
                        maxvalue = 60 * 60 * 24 * 10 ** 9 - 1
                        if value < 0 or value >= maxvalue:
                            raise BadInputException(
                                f"value for type={spec['option_id']} is out of range 0 < X < {maxvalue} for value={value!r}"
                            )

                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.LONG}", 8, value
                        )

                    elif spec["option_id"] in (OptionID.DATE,):
                        # what about buffer
                        if not isinstance(value, datetime.date):
                            raise BadInputException(
                                f"expected type=datetime.date but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.UINT}",
                            4,
                            (
                                value
                                - datetime.datetime.fromtimestamp(
                                    0, datetime.timezone.utc
                                ).date()
                            ).days
                            + 2 ** 31,
                        )
                    elif spec["option_id"] in (OptionID.TIMESTAMP,):
                        # what about buffer
                        if not isinstance(value, datetime.datetime):
                            raise BadInputException(
                                f"expected type=datetime.date but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.LONG}",
                            8,
                            round(
                                value.replace(tzinfo=datetime.timezone.utc).timestamp()
                                * 10 ** 3
                            ),
                        )
                    elif spec["option_id"] in (OptionID.TIMEUUID, OptionID.UUID):
                        # what about buffer
                        if not isinstance(value, uuid.UUID):
                            raise BadInputException(
                                f"expected type=uuid.UUID but got type={pretty_type(value)} for value={value!r}"
                            )
                        if (
                            spec["option_id"] == OptionID.TIMEUUID
                            and value.version != 1
                        ):
                            raise BadInputException(
                                f"value for type={spec['option_id']} is not UUID version 1, but version={value.version}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}", 16
                        ) + value.int.to_bytes(length=16, byteorder="big")

                    elif spec["option_id"] in (OptionID.BOOLEAN,):
                        # what about buffer
                        if not isinstance(value, bool):
                            raise BadInputException(
                                f"expected type=bool but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += pack(
                            f"{NETWORK_ORDER}{STypes.INT}{STypes.BYTE}",
                            1,
                            (0, 1)[value],
                        )

                    elif spec["option_id"] in (OptionID.ASCII, OptionID.VARCHAR):
                        if not isinstance(value, str):
                            raise BadInputException(
                                f"expected type=str but got type={pretty_type(value)} for value={value!r}"
                            )
                        body += encode_long_string(value)
                    elif spec["option_id"] in (OptionID.DOUBLE,):
                        if not isinstance(value, float):
                            raise BadInputException(
                                f"expected type=float but got type={pretty_type(value)} for value={value!r}"
                            )
                        encoded = pack(f"{NETWORK_ORDER}{STypes.DOUBLE}", value)
                        body += encode_int(len(encoded)) + encoded
                    elif spec["option_id"] in (OptionID.FLOAT,):
                        if not isinstance(value, float):
                            raise BadInputException(
                                f"expected type=float but got type={pretty_type(value)} for value={value!r}"
                            )
                        encoded = pack(f"{NETWORK_ORDER}{STypes.FLOAT}", value)
                        body += encode_int(len(encoded)) + encoded

                    elif spec["option_id"] in (OptionID.INET,):
                        if not isinstance(
                            value, ipaddress.IPv4Address
                        ) and not isinstance(value, ipaddress.IPv6Address):
                            raise BadInputException(
                                f"expected type=ipaddress.IPv4Address/ipaddress.IPv6Address but got "
                                + "type={pretty_type(value)} for value={value!r}"
                            )
                        length = (4, 16)[value.version == 6]
                        body += encode_int(length)
                        body += int(value).to_bytes(
                            length, byteorder="big", signed=True
                        )
                    elif spec["option_id"] in (OptionID.DECIMAL,):
                        if not isinstance(value, decimal.Decimal):
                            raise BadInputException(
                                f"expected type=decimal.Decimal but got type={pretty_type(value)} for value={value!r}"
                            )
                        scale = encode_int(-1 * value.as_tuple().exponent)
                        unscaled = encode_varint(
                            sum(
                                10 ** c * d
                                for c, d in enumerate(reversed(value.as_tuple().digits))
                            )
                        )
                        body += encode_int(len(scale + unscaled)) + scale + unscaled
                    else:
                        raise InternalDriverError(
                            f"cannot handle unknown option_id=0x{spec['option_id']:x}"
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
