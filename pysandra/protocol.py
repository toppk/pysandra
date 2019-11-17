from struct import pack, unpack, Struct
from enum import Enum
from .exceptions import TypeViolation
from .utils import get_logger
from .constants import Opcode, Consitency, Flags

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
            return TypeViolation("should be bytes")
        return f"{Types.SHORT}{len(text)}s"

    def LongString(text):
        if not isinstance(text, bytes):
            return TypeViolation("should be bytes")
        return f"{Types.INT}{len(text)}s"


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
    def __init__(self):
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
    def __init__(self):
        pass


class ReadyResponse(ResponseMessage):
    opcode = Opcode.READY

    def __init__(self, version=None, flags=None):
        self.version = version
        self.flags = flags

    @staticmethod
    def build(version=None, flags=None, body=None):
        msg = ReadyResponse(version=version, flags=flags)
        return msg


class ResultResponse(ResponseMessage):
    opcode = Opcode.RESULT

    def __init__(self, version=None, flags=None, body=None):
        self.version = version
        self.flags = flags
        self.body = body

    @staticmethod
    def build(version=None, flags=None, body=None):
        msg = ResultResponse(version=version, flags=flags, body=body)
        return msg


class StartupRequest(RequestMessage):
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


class QueryRequest(RequestMessage):
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
            Consitency.ONE, Flags.SKIP_METADATA
        )
        # query_body += pack("!HL", Consitency.ONE, 0x0002)
        query_head = self._header_bytes(query_body, stream_id)
        query_send = query_head + query_body
        logger.debug(f"msg={query_send}")
        return query_send
