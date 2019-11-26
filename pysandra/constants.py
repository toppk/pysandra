# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
from enum import Enum

from .core import HexEnum

# driver

# max event queue size
EVENTS_QUEUE_MAXSIZE = 100

# in bytes
COMPRESS_MINIMUM = 60

# in seconds
STARTUP_TIMEOUT = 10
REQUEST_TIMEOUT = 10

# wire protocol

CQL_VERSION = "3.0.0"
SERVER_SENT = 0x80
EVENT_STREAM_ID = -1
PREFERRED_ALGO = "lz4"

# connection
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9042


class Opcode(HexEnum):
    ERROR = 0x00
    STARTUP = 0x01
    READY = 0x02
    AUTHENTICATE = 0x03
    OPTIONS = 0x05
    SUPPORTED = 0x06
    QUERY = 0x07
    RESULT = 0x08
    PREPARE = 0x09
    EXECUTE = 0x0A
    REGISTER = 0x0B
    EVENT = 0x0C
    BATCH = 0x0D
    AUTH_CHALLENGE = 0x0E
    AUTH_RESPONSE = 0x0F
    AUTH_SUCCESS = 0x10


class Kind(HexEnum):
    VOID = 0x0001
    ROWS = 0x0002
    SET_KEYSPACE = 0x0003
    PREPARED = 0x0004
    SCHEMA_CHANGE = 0x0005


class Events(str, Enum):
    TOPOLOGY_CHANGE = "TOPOLOGY_CHANGE"
    STATUS_CHANGE = "STATUS_CHANGE"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"


class Options(str, Enum):
    CQL_VERSION = "CQL_VERSION"
    COMPRESSION = "COMPRESSION"
    NO_COMPACT = "NO_COMPACT"
    THROW_ON_OVERLOAD = "THROW_ON_OVERLOAD"


class Consistency(HexEnum):
    ANY = 0x0000
    ONE = 0x0001
    TWO = 0x0002
    THREE = 0x0003
    QUORUM = 0x0004
    ALL = 0x0005
    LOCAL_QUORUM = 0x0006
    EACH_QUORUM = 0x0007
    SERIAL = 0x0008
    LOCAL_SERIAL = 0x0009
    LOCAL_ONE = 0x000A


class ResultFlags(HexEnum):
    GLOBAL_TABLES_SPEC = 0x01
    HAS_MORE_PAGES = 0x02
    NO_METADATA = 0x04


class OptionID(HexEnum):
    CUSTOM = 0x0000
    ASCII = 0x0001
    BIGINT = 0x0002
    BLOB = 0x0003
    BOOLEAN = 0x0004
    COUNTER = 0x0005
    DECIMAL = 0x0006
    DOUBLE = 0x0007
    FLOAT = 0x0008
    INT = 0x0009
    TIMESTAMP = 0x000B
    UUID = 0x000C
    VARCHAR = 0x000D
    VARINT = 0x000E
    TIMEUUID = 0x000F
    INET = 0x0010
    DATE = 0x0011
    TIME = 0x0012
    SMALLINT = 0x0013
    TINYINT = 0x0014
    LIST = 0x0020
    MAP = 0x0021
    SET = 0x0022
    UDT = 0x0030
    TUPLE = 0x0031


class QueryFlags(HexEnum):
    VALUES = 0x01
    SKIP_METADATA = 0x02
    PAGE_SIZE = 0x04
    WITH_PAGING_STATE = 0x08
    WITH_SERIAL_CONSITENCY = 0x10
    WITH_DEFAULT_TIMESTAMP = 0x20
    WITH_NAMES_FOR_VALUES = 0x40


class Flags(HexEnum):
    COMPRESSION = 0x01
    TRACING = 0x02
    CUSTOM_PAYLOAD = 0x04
    WARNING = 0x08


class ErrorCode(HexEnum):
    SERVER_ERROR = 0x0000
    PROTOCOL_ERROR = 0x000A
    AUTHENTICATION_ERROR = 0x0100
    UNAVAILABLE_EXCEPTION = 0x1000
    OVERLOADED = 0x1001
    IS_BOOTSTRAPPING = 0x1002
    TRUNCATE_ERROR = 0x1003
    WRITE_TIMEOUT = 0x1100
    READ_TIMEOUT = 0x1200
    READ_FAILURE = 0x1300
    FUNCTION_FAILURE = 0x1400
    WRITE_FAILURE = 0x1500
    SYNTAX_ERROR = 0x2000
    UNAUTHORIZED = 0x2100
    INVALID = 0x2200
    CONFIG_ERROR = 0x2300
    ALREADY_EXISTS = 0x2400
    UNPREPARED = 0x2500


class NodeStatus(str, Enum):
    ON = "ON"
    OFF = "OFF"


class TopologyStatus(str, Enum):
    NEW_NODE = "NEW_NODE"
    REMOVED_NODE = "REMOVED_NODE"


class SchemaChangeType(str, Enum):
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DROPPED = "DROPPED"


class SchemaChangeTarget(str, Enum):
    KEYSPACE = "KEYSPACE"
    TABLE = "TABLE"
    TYPE = "TYPE"
    FUNCTION = "FUNCTION"
    AGGREGATE = "AGGREGATE"


class WriteType(str, Enum):
    SIMPLE = "SIMPLE"
    BATCH = "BATCH"
    UNLOGGED_BATCH = "UNLOGGED_BATCH"
    COUNTER = "COUNTER"
    BATCH_LOG = "BATCH_LOG"
    CAS = "CAS"
    VIEW = "VIEW"
    CDC = "CDC"
