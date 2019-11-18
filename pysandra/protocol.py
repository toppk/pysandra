from struct import pack, unpack, Struct
from enum import Enum
from .exceptions import TypeViolation, UnknownPayloadException, InternalDriverError
from .utils import get_logger, SBytes
from .constants import Opcode, Consitency, QueryFlags, Flags, Kind, ResultFlags
from .types import Rows

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


class Types(str, Enum):
    NetOrder = "!"
    INT = "l"
    LONG = "q"
    SHORT = "H"
    BYTE = "B"

    def String(text):
        if not isinstance(text, bytes):
            raise TypeViolation(f"text={text} should be bytes")
        return f"{Types.SHORT}{len(text)}s"

    def LongString(text):
        if not isinstance(text, bytes):
            raise TypeViolation(f"text={text} should be bytes")
        return f"{Types.INT}{len(text)}s"

    def Bytes(count):
        return f"{count}s"


structs = None


def get_struct(fmt):
    global structs
    if structs is None:
        structs = {}
        formats = [
            f"{NETWORK_ORDER}{Types.SHORT}",
            f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}",
            f"{NETWORK_ORDER}{Types.SHORT}{Types.BYTE}",
        ]
        for frmt in formats:
            structs[frmt] = Struct(frmt)
    if fmt not in structs:
        raise InternalDriverError(f"format={fmt} not cached")
    return structs[fmt]


class Protocol:
    pass


class BaseMessage:
    pass


class RequestMessage(BaseMessage):
    def __init__(self, version=None, flags=None, stream_id=None):
        self.version = version
        self.flags = flags
        self.stream_id = stream_id

    def _header_bytes(self, body, stream_id=None):
        if stream_id is None:
            stream_id = self.stream_id
        return get_struct(
            f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}"
        ).pack(self.version, self.flags, stream_id, self.opcode, len(body))


class ResponseMessage(BaseMessage):
    def __init__(self, version=None, flags=None, stream_id=None):
        self.version = version
        self.flags = flags


class ReadyMessage(ResponseMessage):
    opcode = Opcode.READY

    @staticmethod
    def build(version=None, flags=None, body=None):
        logger.debug(f"ReadyResponse body={body}")
        if body != b"":
            raise UnknownPayloadException(f"READY message should have no payload")
        msg = ReadyMessage(flags=flags)
        return msg


class ResultMessage(ResponseMessage):
    opcode = Opcode.RESULT

    def __init__(self, kind=None, **kwargs):
        super().__init__(**kwargs)
        self.kind = kind

    @staticmethod
    def build(version=None, flags=None, query_flags=None, body=None):
        msg = None
        body = SBytes(body)
        kind = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
        logger.debug(f"ResultResponse kind={kind} body={body}")
        if kind == Kind.VOID:
            msg = VoidResultMessage(version=version, flags=flags, kind=kind)
        elif kind == Kind.ROWS:
            result_flags = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
            column_count = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
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
                pass
            # parse rows
            rows = Rows(column_count=column_count)
            rows_count = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
            for i in range(rows_count * column_count):
                if body.at_end():
                    raise InternalDriverError(f"body at end")
                length = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
                cell = None
                if length > 0:
                    cell = unpack(
                        f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
                    )
                elif length == 0:
                    cell = ""
                rows.add(cell)
            msg = RowsResultMessage(version=version, flags=flags, kind=kind)
            msg.rows = rows

        elif kind == Kind.SET_KEYSPACE:
            pass
        elif kind == Kind.PREPARED:
            # <id>
            length = unpack(f"{NETWORK_ORDER}{Types.SHORT}", body.show(2))[0]
            if length < 1:
                raise InternalDriverError(
                    f"cannot store prepared query id with length={length}"
                )
            query_id = unpack(
                f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
            )[0]
            ## <metadata>
            # <flags>
            flags = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
            # <columns_count>
            columns_count = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
            # <pk_count>
            pk_count = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
            pk_index = None
            if pk_count > 0:
                pk_index = list(
                    unpack(
                        f"{NETWORK_ORDER}{Types.SHORT * pk_count}",
                        body.show(2 * pk_count),
                    )
                )
            logger.debug(
                f"build query_id={query_id} flags={flags} columns_count={columns_count} pk_count={pk_count} pk_index={pk_index}"
            )
            # <global_table_spec>
            if flags & ResultFlags.GLOBAL_TABLES_SPEC != 0:
                # keyspace
                length = unpack(f"{NETWORK_ORDER}{Types.SHORT}", body.show(2))[0]
                keyspace = unpack(
                    f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
                )[0].decode("utf-8")
                # table
                length = unpack(f"{NETWORK_ORDER}{Types.SHORT}", body.show(2))[0]
                table = unpack(
                    f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
                )[0].decode("utf-8")
                logger.debug(f"build keyspace={keyspace} table={table}")
            # <col_spec_i>
            col_specs = []
            if columns_count > 0:
                for col in range(columns_count):
                    col_spec = {}
                    if flags & ResultFlags.GLOBAL_TABLES_SPEC == 0:
                        # <ksname><tablename>
                        length = unpack(f"{NETWORK_ORDER}{Types.SHORT}", body.show(2))[
                            0
                        ]
                        col_spec["ksname"] = unpack(
                            f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
                        )[0].decode("utf-8")
                        length = unpack(f"{NETWORK_ORDER}{Types.SHORT}", body.show(2))[
                            0
                        ]
                        col_spec["tablename"] = unpack(
                            f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
                        )[0].decode("utf-8")
                    # <name><type>
                    length = unpack(f"{NETWORK_ORDER}{Types.SHORT}", body.show(2))[0]
                    col_spec["name"] = unpack(
                        f"{NETWORK_ORDER}{Types.Bytes(length)}", body.show(length)
                    )[0].decode("utf-8")
                    # <type>
                    col_spec["option_id"] = unpack(
                        f"{NETWORK_ORDER}{Types.SHORT}", body.show(2)
                    )[0]
                    if col_spec["option_id"] < 0x0001 or col_spec["option_id"] > 0x0014:
                        raise InternalDriverError(
                            f"unhandled option_id={col_spec['option_id']}"
                        )
            # <result_metadata>
            # <flags>
            result_flags = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[0]
            # <columns_count>
            result_columns_count = unpack(f"{NETWORK_ORDER}{Types.INT}", body.show(4))[
                0
            ]
            if result_flags & ResultFlags.HAS_MORE_PAGES != 0x00:
                # parse paging state
                pass
            if result_flags & ResultFlags.NO_METADATA == 0x00:
                if result_flags & ResultFlags.GLOBAL_TABLES_SPEC != 0x00:
                    # parse global_table_spec
                    pass
                # parse col_spec_i
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
                f"ResultResponse no msg generated for body={body}"
            )
        if not body.at_end():
            raise InternalDriverError(
                f"ResultResponse still data left remains={body.show()}"
            )
        return msg


class RowsResultMessage(ResultMessage):
    def __init__(self, rows=None, **kwargs):
        super().__init__(**kwargs)
        self.rows = rows


class VoidResultMessage(ResultMessage):
    pass


class PreparedResultMessage(ResultMessage):
    def __init__(self, query_id=None, col_specs=None, **kwargs):
        super().__init__(**kwargs)
        self.col_specs = col_specs
        self.query_id = query_id


class StartupMessage(RequestMessage):
    opcode = Opcode.STARTUP

    def __init__(self, options=None, **kwargs):
        super().__init__(**kwargs)
        self.options = options

    def to_bytes(self, stream_id=None):
        if stream_id is None:
            stream_id = self.stream_id
        startup_body = get_struct(f"{NETWORK_ORDER}{Types.SHORT}").pack(
            len(self.options)
        )
        for key, value in self.options.items():
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
            startup_body += pack(
                f"{NETWORK_ORDER}{Types.String(key_bytes)}{Types.String(value_bytes)}",
                len(key_bytes),
                key_bytes,
                len(value_bytes),
                value_bytes,
            )
        test = get_struct(
            f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}"
        )
        startup_head = self._header_bytes(startup_body, stream_id=stream_id)
        startup_send = startup_head + startup_body
        logger.debug(f"msg={startup_send}")
        return startup_send


class ExecuteMessage(RequestMessage):
    opcode = Opcode.EXECUTE

    def __init__(self, query_id=None, query_params=None, **kwargs):
        self.query_id = query_id
        self.query_params = query_params
        super().__init__(**kwargs)

    def to_bytes(self, stream_id=None):
        if stream_id is None:
            stream_id = self.stream_id
        # <id>
        execute_body = pack(
            f"{NETWORK_ORDER}{Types.String(self.query_id)}",
            len(self.query_id),
            self.query_id,
        )
        ##   <query_parameters>
        #     <consistency><flags>
        execute_body += get_struct(f"{NETWORK_ORDER}{Types.SHORT}{Types.BYTE}").pack(
            Consitency.ONE, QueryFlags.SKIP_METADATA
        )
        #     [<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
        execute_head = self._header_bytes(execute_body, stream_id)
        msg_bytes = execute_head + execute_body
        logger.debug(f"msg_bytes={msg_bytes}")
        return msg_bytes


class QueryMessage(RequestMessage):
    opcode = Opcode.QUERY

    def __init__(self, query=None, **kwargs):
        self.query = query
        super().__init__(**kwargs)

    def to_bytes(self, stream_id=None):
        if stream_id is None:
            stream_id = self.stream_id
        query = self.query.encode("utf-8")
        query_body = pack(
            f"{NETWORK_ORDER}{Types.LongString(query)}", len(query), query
        )
        #   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>][<keyspace>][<now_in_seconds>]
        query_body += get_struct(f"{NETWORK_ORDER}{Types.SHORT}{Types.BYTE}").pack(
            Consitency.ONE, QueryFlags.SKIP_METADATA
        )
        # query_body += pack("!HL", Consitency.ONE, 0x0002)
        query_head = self._header_bytes(query_body, stream_id)
        msg_bytes = query_head + query_body
        logger.debug(f"msg_bytes={msg_bytes}")
        return msg_bytes


class PrepareMessage(RequestMessage):
    opcode = Opcode.PREPARE

    def __init__(self, query=None, **kwargs):
        self.query = query
        super().__init__(**kwargs)

    def to_bytes(self, stream_id=None):
        if stream_id is None:
            stream_id = self.stream_id
        query = self.query.encode("utf-8")
        prepare_body = pack(
            f"{NETWORK_ORDER}{Types.LongString(query)}", len(query), query
        )
        prepare_head = self._header_bytes(prepare_body, stream_id)
        msg_bytes = prepare_head + prepare_body
        logger.debug(f"PrepareMessage msg_bytes={msg_bytes}")
        return msg_bytes
