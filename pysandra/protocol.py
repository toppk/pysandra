from enum import Enum
from struct import Struct, pack, unpack
from typing import Any, List, Optional, Tuple, Union

from .constants import Consitency, Kind, Opcode, OptionID, QueryFlags, ResultFlags
from .exceptions import (
    BadInputException,
    InternalDriverError,
    TypeViolation,
    UnknownPayloadException,
)
from .types import Rows
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
    SHORT = "H"
    BYTE = "B"

    @staticmethod
    def String(text: bytes) -> str:
        if not isinstance(text, bytes):
            raise TypeViolation(f"text={text} should be bytes")
        return f"{STypes.SHORT}{len(text)}s"

    @staticmethod
    def LongString(text: bytes) -> str:
        if not isinstance(text, bytes):
            raise TypeViolation(f"text={text} should be bytes")
        return f"{STypes.INT}{len(text)}s"

    @staticmethod
    def Bytes(count: int) -> str:
        return f"{count}s"


structs: dict = {}


def get_struct(fmt: str) -> Struct:
    global structs
    if len(structs) == 0:
        formats = [
            f"{NETWORK_ORDER}{STypes.SHORT}",
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}",
            f"{NETWORK_ORDER}{STypes.SHORT}{STypes.BYTE}",
        ]
        for frmt in formats:
            structs[frmt] = Struct(frmt)
    if fmt not in structs:
        raise InternalDriverError(f"format={fmt} not cached")
    return structs[fmt]


class Protocol:
    def decode_header(self, header: bytes) -> Tuple[int, int, int, int, int]:
        pass

    def startup(self, stream_id: int = None, params: dict = None) -> "StartupMessage":
        pass

    def build_response(
        self,
        request: "RequestMessage",
        version: int,
        flags: int,
        stream: int,
        opcode: int,
        length: int,
        body: bytes,
    ) -> Union[bool, "Rows", bytes]:
        pass

    def query(self, stream_id: int = None, params: dict = None) -> "QueryMessage":
        pass

    def execute(self, stream_id: int = None, params: dict = None) -> "ExecuteMessage":
        pass

    def prepare(self, stream_id: int = None, params: dict = None) -> "PrepareMessage":
        pass


class BaseMessage:
    pass


class RequestMessage(BaseMessage):
    opcode: int

    def __init__(
        self, version: int = None, flags: int = None, stream_id: int = None
    ) -> None:
        self.version = version
        self.flags = flags
        self.stream_id = stream_id

    def _header_bytes(self, body: bytes, stream_id: int = None) -> bytes:
        if stream_id is None:
            stream_id = self.stream_id
        return get_struct(
            f"{NETWORK_ORDER}{STypes.BYTE}{STypes.BYTE}{STypes.SHORT}{STypes.BYTE}{STypes.INT}"
        ).pack(self.version, self.flags, stream_id, self.opcode, len(body))


class ResponseMessage(BaseMessage):
    opcode: int

    def __init__(
        self, version: int = None, flags: int = None, stream_id: int = None
    ) -> None:
        self.version = version
        self.flags = flags


class ReadyMessage(ResponseMessage):
    opcode = Opcode.READY

    @staticmethod
    def build(
        version: int = None, flags: int = None, body: bytes = None
    ) -> "ReadyMessage":
        assert body is not None
        sbody = SBytes(body)
        logger.debug(f"ReadyResponse body={sbody!r}")
        if not sbody.at_end():
            raise UnknownPayloadException(
                f"READY message should have no payload={sbody.show()!r}"
            )
        return ReadyMessage(flags=flags)


class ErrorMessage(ResponseMessage):
    opcode = Opcode.ERROR

    def __init__(
        self, error_code: int = None, error_text: str = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.error_code = error_code
        self.error_text = error_text

    @staticmethod
    def build(
        version: int = None, flags: int = None, body: bytes = None
    ) -> "ErrorMessage":
        assert body is not None
        sbody = SBytes(body)
        logger.debug(f"ErrorResponse body={sbody!r}")
        error_code = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
        logger.debug(f"ErrorMessage error_code={error_code:x}")
        length = unpack(f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2))[0]
        error_text = unpack(
            f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
        )[0].decode("utf-8")
        msg = ErrorMessage(flags=flags, error_code=error_code, error_text=error_text)
        if not sbody.at_end():
            raise InternalDriverError(
                f"ErrorMessage still data left remains={sbody.show()!r}"
            )
        return msg


class ResultMessage(ResponseMessage):
    opcode = Opcode.RESULT

    def __init__(self, kind: int = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.kind = kind

    @staticmethod
    def build(
        version: int = None,
        flags: int = None,
        query_flags: int = None,
        body: bytes = None,
    ) -> "ResultMessage":
        assert body is not None
        sbody = SBytes(body)
        msg: Optional["ResultMessage"] = None
        kind = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
        logger.debug(f"ResultResponse kind={kind} sbody={sbody!r}")
        if kind == Kind.VOID:
            msg = VoidResultMessage(version=version, flags=flags, kind=kind)
        elif kind == Kind.ROWS:
            result_flags = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            column_count = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            logger.debug(
                f"ResultResponse result_flags={result_flags} column_count={column_count}"
            )
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                # parse paging state
                pass
            if result_flags & ResultFlags.NO_METADATA == 0x00:
                if result_flags & ResultFlags.GLOBAL_TABLES_SPEC != 0x00:
                    # parse global_table_spec
                    pass
                # parse col_spec_i
            # parse rows
            rows = Rows(column_count=column_count)
            rows_count = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            for _cnt in range(rows_count * column_count):
                if sbody.at_end():
                    raise InternalDriverError(f"sbody at end")
                length = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
                cell: bytes = b""
                if length > 0:
                    cell = unpack(
                        f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
                    )[0]
                elif length == 0:
                    cell = b""
                rows.add(cell)
            msg = RowsResultMessage(version=version, flags=flags, kind=kind)
            msg.rows = rows

        elif kind == Kind.SET_KEYSPACE:
            pass
        elif kind == Kind.PREPARED:
            # <id>
            length = unpack(f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2))[0]
            if length < 1:
                raise InternalDriverError(
                    f"cannot store prepared query id with length={length}"
                )
            query_id = unpack(
                f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
            )[0]
            # <metadata>
            # <flags>
            flags = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            # <columns_count>
            columns_count = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            # <pk_count>
            pk_count = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            pk_index = None
            if pk_count > 0:
                pk_index = list(
                    unpack(
                        f"{NETWORK_ORDER}{STypes.SHORT * pk_count}",
                        sbody.show(2 * pk_count),
                    )
                )
            logger.debug(
                f"build query_id={query_id} flags={flags} columns_count={columns_count} pk_count={pk_count} pk_index={pk_index}"
            )
            # <global_table_spec>
            assert flags is not None
            if flags & ResultFlags.GLOBAL_TABLES_SPEC != 0:
                # keyspace
                length = unpack(f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2))[0]
                keyspace = unpack(
                    f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
                )[0].decode("utf-8")
                # table
                length = unpack(f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2))[0]
                table = unpack(
                    f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
                )[0].decode("utf-8")
                logger.debug(f"build keyspace={keyspace} table={table}")
            # <col_spec_i>
            col_specs = []
            if columns_count > 0:
                for _col in range(columns_count):
                    col_spec = {}
                    if flags & ResultFlags.GLOBAL_TABLES_SPEC == 0:
                        # <ksname><tablename>
                        length = unpack(
                            f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2)
                        )[0]
                        col_spec["ksname"] = unpack(
                            f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
                        )[0].decode("utf-8")
                        length = unpack(
                            f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2)
                        )[0]
                        col_spec["tablename"] = unpack(
                            f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
                        )[0].decode("utf-8")
                    # <name><type>
                    length = unpack(f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2))[0]
                    col_spec["name"] = unpack(
                        f"{NETWORK_ORDER}{STypes.Bytes(length)}", sbody.show(length)
                    )[0].decode("utf-8")
                    # <type>
                    col_spec["option_id"] = unpack(
                        f"{NETWORK_ORDER}{STypes.SHORT}", sbody.show(2)
                    )[0]
                    if col_spec["option_id"] < 0x0001 or col_spec["option_id"] > 0x0014:
                        raise InternalDriverError(
                            f"unhandled option_id={col_spec['option_id']}"
                        )
                    col_specs.append(col_spec)
            # <result_metadata>
            # <flags>
            result_flags = unpack(f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4))[0]
            # <columns_count>
            result_columns_count = unpack(
                f"{NETWORK_ORDER}{STypes.INT}", sbody.show(4)
            )[0]
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                # parse paging state
                pass
            if result_flags & ResultFlags.NO_METADATA == 0x00:
                if result_flags & ResultFlags.GLOBAL_TABLES_SPEC != 0x00:
                    # parse global_table_spec
                    pass
                # parse col_spec_i
                for _col in range(result_columns_count):
                    pass
            msg = PreparedResultMessage(
                version=version,
                flags=flags,
                kind=kind,
                col_specs=col_specs,
                query_id=query_id,
            )
        elif kind == Kind.SCHEMA_CHANGE:
            pass
        else:
            raise UnknownPayloadException(f"RESULT message has unknown kind={kind}")
        if msg is None:
            raise InternalDriverError(
                f"ResultResponse no msg generated for sbody={sbody!r}"
            )
        if not sbody.at_end():
            raise InternalDriverError(
                f"ResultResponse still data left remains={sbody.show()!r}"
            )
        return msg


class RowsResultMessage(ResultMessage):
    rows: "Rows"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class VoidResultMessage(ResultMessage):
    pass


class PreparedResultMessage(ResultMessage):
    query_id: bytes

    def __init__(
        self, query_id: bytes = None, col_specs: List[dict] = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.col_specs = col_specs
        assert query_id is not None
        self.query_id = query_id


class StartupMessage(RequestMessage):
    opcode = Opcode.STARTUP

    def __init__(self, options: dict = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.options = options

    def to_bytes(self, stream_id: int = None) -> bytes:
        if stream_id is None:
            stream_id = self.stream_id
        assert self.options is not None
        startup_body = get_struct(f"{NETWORK_ORDER}{STypes.SHORT}").pack(
            len(self.options)
        )
        for key, value in self.options.items():
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
            startup_body += pack(
                f"{NETWORK_ORDER}{STypes.String(key_bytes)}{STypes.String(value_bytes)}",
                len(key_bytes),
                key_bytes,
                len(value_bytes),
                value_bytes,
            )
        startup_head = self._header_bytes(startup_body, stream_id=stream_id)
        startup_send = startup_head + startup_body
        logger.debug(f"msg={startup_send!r}")
        return startup_send


class ExecuteMessage(RequestMessage):
    opcode = Opcode.EXECUTE

    def __init__(
        self,
        query_id: bytes = None,
        query_params: dict = None,
        col_specs: List[dict] = None,
        **kwargs: Any,
    ) -> None:
        self.query_id = query_id
        self.query_params = query_params
        self.col_specs = col_specs
        super().__init__(**kwargs)

    def to_bytes(self, stream_id: int = None) -> bytes:
        if stream_id is None:
            stream_id = self.stream_id
        # data check
        assert self.col_specs is not None
        assert self.query_params is not None
        if len(self.col_specs) != len(self.query_params):
            raise BadInputException(
                f" count of execute params={len(self.query_params)} doesn't match prepared statement count={len(self.col_specs)}"
            )
        # <id>
        assert self.query_id is not None
        execute_body = pack(
            f"{NETWORK_ORDER}{STypes.String(self.query_id)}",
            len(self.query_id),
            self.query_id,
        )
        #   <query_parameters>
        #     <consistency><flags>
        execute_body += get_struct(f"{NETWORK_ORDER}{STypes.SHORT}{STypes.BYTE}").pack(
            Consitency.ONE, QueryFlags.VALUES
        )
        #     [<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
        # <n>
        execute_body += get_struct(f"{NETWORK_ORDER}{STypes.SHORT}").pack(
            len(self.col_specs)
        )
        for value, spec in zip(self.query_params, self.col_specs):
            if spec["option_id"] == OptionID.INT:
                execute_body += pack(
                    f"{NETWORK_ORDER}{STypes.INT}{STypes.INT}", 4, value
                )
            elif spec["option_id"] == OptionID.VARCHAR:
                value_bytes = value.encode("utf-8")
                execute_body += pack(
                    f"{NETWORK_ORDER}{STypes.LongString(value_bytes)}",
                    len(value_bytes),
                    value_bytes,
                )
            else:
                raise InternalDriverError(
                    f"cannot handle unknown option_id={spec['option_id']}"
                )
        execute_head = self._header_bytes(execute_body, stream_id)
        msg_bytes = execute_head + execute_body
        logger.debug(f"msg_bytes={msg_bytes!r}")
        return msg_bytes


class QueryMessage(RequestMessage):
    opcode = Opcode.QUERY

    def __init__(self, query: str = None, **kwargs: Any) -> None:
        self.query = query
        super().__init__(**kwargs)

    def to_bytes(self, stream_id: int = None) -> bytes:
        if stream_id is None:
            stream_id = self.stream_id
        assert self.query is not None
        query_bytes = self.query.encode("utf-8")
        query_body = pack(
            f"{NETWORK_ORDER}{STypes.LongString(query_bytes)}",
            len(query_bytes),
            query_bytes,
        )
        #   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>][<keyspace>][<now_in_seconds>]
        query_body += get_struct(f"{NETWORK_ORDER}{STypes.SHORT}{STypes.BYTE}").pack(
            Consitency.ONE, QueryFlags.SKIP_METADATA
        )
        # query_body += pack("!HL", Consitency.ONE, 0x0002)
        query_head = self._header_bytes(query_body, stream_id)
        msg_bytes = query_head + query_body
        logger.debug(f"msg_bytes={msg_bytes!r}")
        return msg_bytes


class PrepareMessage(RequestMessage):
    opcode = Opcode.PREPARE

    def __init__(self, query: str = None, **kwargs: Any) -> None:
        self.query = query
        super().__init__(**kwargs)

    def to_bytes(self, stream_id: int = None) -> bytes:
        if stream_id is None:
            stream_id = self.stream_id
        assert self.query is not None
        query_bytes = self.query.encode("utf-8")
        prepare_body = pack(
            f"{NETWORK_ORDER}{STypes.LongString(query_bytes)}",
            len(query_bytes),
            query_bytes,
        )
        prepare_head = self._header_bytes(prepare_body, stream_id)
        msg_bytes = prepare_head + prepare_body
        logger.debug(f"PrepareMessage msg_bytes={msg_bytes!r}")
        return msg_bytes
