import pytest

from pysandra.core import SBytes
from pysandra.exceptions import InternalDriverError
from pysandra.protocol import (
    decode_int,
    decode_int_bytes,
    decode_short,
    decode_short_bytes,
    decode_string,
    decode_string_multimap,
    decode_strings_list,
    encode_int,
    encode_long_string,
    encode_short,
    encode_string,
    encode_strings_list,
    encode_value,
    get_struct,
)


def test_get_bad_struct():
    with pytest.raises(InternalDriverError, match=r"not cached"):
        get_struct("!Hqqq")


def test_encode_short():
    body = 2
    assert encode_short(body) == b"\00\02"


def test_encode_int():
    body = 5
    assert encode_int(body) == b"\00\00\00\05"


def test_encode_string():
    body = "test"
    assert encode_string(body) == b"\00\04test"


def test_encode_string_bytes():
    body = b"test"
    assert encode_string(body) == b"\00\04test"


def test_encode_value_string():
    body = "asdf"
    assert encode_value(body) == b"\x00\x00\x00\x04asdf"


def test_encode_value_int():
    body = 123
    assert encode_value(body) == b"\x00\x00\x00\x04\x00\x00\x00\x7b"


def test_encode_value_bytes():
    body = b"asdf"
    assert encode_value(body) == b"\x00\x00\x00\x04asdf"


def test_encode_value_none():
    body = None
    assert encode_value(body) == b"\xff\xff\xff\xff"


def test_encode_long_string_plain():
    body = "longer"
    assert encode_long_string(body) == b"\00\00\00\06longer"


def test_encode_long_string_bytes():
    body = b"longer"
    assert encode_long_string(body) == b"\00\00\00\06longer"


def test_encode_strings_list():
    body = ["apple", "orange", "book"]
    assert encode_strings_list(body) == b"\00\03\00\05apple\00\06orange\00\04book"


def test_decode_string():
    body = SBytes(b"\00\04asdf")
    assert decode_string(body) == "asdf"


def test_decode_short():
    body = SBytes(b"\00\04")
    assert decode_short(body) == 4


def test_decode_int():
    body = SBytes(b"\00\04\02\04")
    assert decode_int(body) == 262660


def test_decode_short_bytes():
    body = SBytes(b"\00\04asdf")
    assert decode_short_bytes(body) == b"asdf"


def test_decode_short_bytes_empty():
    body = SBytes(b"\00\00")
    assert decode_short_bytes(body) == b""


def test_decode_int_bytes_zero():
    body = SBytes(b"\00\00\00\00")
    assert decode_int_bytes(body) == b""


def test_decode_int_bytes_null():
    body = SBytes(b"\xff\xff\xff\xff")
    assert decode_int_bytes(body) is None


def test_decode_int_bytes_plain():
    body = SBytes(b"\00\00\00\04asdf")
    assert decode_int_bytes(body) == b"asdf"


def test_decode_strings_list():
    body = SBytes(b"\00\02\00\02as\00\03dfg")
    assert decode_strings_list(body) == ["as", "dfg"]


def test_decode_string_multimap():
    body = SBytes(
        b"\00\03\00\02as\00\02\00\02df\00\03zxc\00\01z\00\03\00\04zxcv\00\01c\00\02vv\00\05last.\00\03\00\03one\00\03two\00\05three"
    )
    assert decode_string_multimap(body) == {
        "as": ["df", "zxc"],
        "z": ["zxcv", "c", "vv"],
        "last.": ["one", "two", "three"],
    }
