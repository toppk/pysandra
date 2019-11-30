import ipaddress
from enum import Enum
from struct import Struct, pack, unpack
from sys import byteorder
from typing import Dict, List, Optional, Union

from .constants import Consistency
from .core import SBytes
from .exceptions import InternalDriverError
from .types import ExpectedType  # noqa: F401
from .types import InetType
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
    UINT = "L"
    LONG = "q"
    SHORT = "h"
    USHORT = "H"
    BYTE = "B"
    CHAR = "s"
    FLOAT = "f"
    DOUBLE = "d"


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


def encode_bytes(value: Union[bytes]) -> bytes:
    value_bytes = value
    return encode_int(len(value_bytes)) + pack(
        f"{NETWORK_ORDER}{len(value_bytes)}{STypes.CHAR}", value_bytes
    )


# https://stackoverflow.com/questions/21017698/converting-int-to-bytes-in-python-3/54141411#54141411
def encode_varint(value: int) -> bytes:
    length = ((value + (value < 0)).bit_length() + 8) // 8
    return value.to_bytes(length, byteorder="big", signed=True)


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


def decode_int_bytes_must(sbytes: "SBytes") -> bytes:
    length = decode_int(sbytes)
    if length == 0:
        return b""
    elif length < 0:
        raise InternalDriverError(f"unexpected negative length")
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
    ipaddr = (
        ipaddress.IPv4Address(intaddress)
        if length == 4
        else ipaddress.IPv6Address(intaddress)
    )
    port = decode_int(sbytes)
    assert isinstance(ipaddr, ipaddress.IPv4Address) or isinstance(
        ipaddr, ipaddress.IPv6Address
    )
    return InetType(ipaddr, port)


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
