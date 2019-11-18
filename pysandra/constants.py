# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
from enum import Enum

## driver

# in seconds
STARTUP_TIMEOUT = 10
REQUEST_TIMEOUT = 10

## wire protocol

CQL_VERSION = "3.0.0"
SERVER_SENT = 0x80


class Opcode(int, Enum):
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


class Kind(int, Enum):
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


class Consitency(int, Enum):
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


class ResultFlags(int, Enum):
    GLOBAL_TABLES_SPEC = 0x01
    HAS_MORE_PAGES = 0x02
    NO_METADATA = 0x04


class QueryFlags(int, Enum):
    SKIP_METADATA = 0x02


class Flags(int, Enum):
    COMPRESSION = 0x01
    TRACING = 0x02
    CUSTOM_PAYLOAD = 0x04
    WARNING = 0x08


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
