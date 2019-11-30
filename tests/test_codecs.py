import pytest

from pysandra import codecs
from pysandra.constants import Consistency
from pysandra.core import SBytes
from pysandra.exceptions import InternalDriverError


def test_codecs_get_bad_struct():
    with pytest.raises(InternalDriverError, match=r"not cached"):
        codecs.get_struct("!Hqqq")


def test_codecs_encode_short():
    body = 2
    assert codecs.encode_short(body) == b"\00\02"


def test_codecs_encode_int():
    body = 5
    assert codecs.encode_int(body) == b"\00\00\00\05"


def test_codecs_encode_string():
    body = "test"
    assert codecs.encode_string(body) == b"\00\04test"


def test_codecs_encode_string_bytes():
    body = b"test"
    assert codecs.encode_string(body) == b"\00\04test"


def test_codecs_encode_value_string():
    body = "asdf"
    assert codecs.encode_value(body) == b"\x00\x00\x00\x04asdf"


def test_codecs_encode_value_int():
    body = 123
    assert codecs.encode_value(body) == b"\x00\x00\x00\x04\x00\x00\x00\x7b"


def test_codecs_encode_value_bytes():
    body = b"asdf"
    assert codecs.encode_value(body) == b"\x00\x00\x00\x04asdf"


def test_codecs_encode_value_none():
    body = None
    assert codecs.encode_value(body) == b"\xff\xff\xff\xff"


def test_codecs_encode_long_string_plain():
    body = "longer"
    assert codecs.encode_long_string(body) == b"\00\00\00\06longer"


def test_codecs_encode_long_string_bytes():
    body = b"longer"
    assert codecs.encode_long_string(body) == b"\00\00\00\06longer"


def test_codecs_encode_strings_list():
    body = ["apple", "orange", "book"]
    assert (
        codecs.encode_strings_list(body) == b"\00\03\00\05apple\00\06orange\00\04book"
    )


def test_codecs_decode_string():
    body = SBytes(b"\00\04asdf")
    assert codecs.decode_string(body) == "asdf"


def test_codecs_decode_short():
    body = SBytes(b"\00\04")
    assert codecs.decode_short(body) == 4


def test_codecs_decode_int():
    body = SBytes(b"\00\04\02\04")
    assert codecs.decode_int(body) == 262660


def test_codecs_decode_short_bytes():
    body = SBytes(b"\00\04asdf")
    assert codecs.decode_short_bytes(body) == b"asdf"


def test_codecs_decode_short_bytes_empty():
    body = SBytes(b"\00\00")
    assert codecs.decode_short_bytes(body) == b""


def test_codecs_decode_int_bytes_zero():
    body = SBytes(b"\00\00\00\00")
    assert codecs.decode_int_bytes(body) == b""


def test_codecs_decode_int_bytes_null():
    body = SBytes(b"\xff\xff\xff\xff")
    assert codecs.decode_int_bytes(body) is None


def test_codecs_decode_int_bytes_plain():
    body = SBytes(b"\00\00\00\04asdf")
    assert codecs.decode_int_bytes(body) == b"asdf"


def test_codecs_decode_strings_list():
    body = SBytes(b"\00\02\00\02as\00\03dfg")
    assert codecs.decode_strings_list(body) == ["as", "dfg"]


def test_codecs_decode_string_multimap():
    body = SBytes(
        b"\00\03\00\02as\00\02\00\02df\00\03zxc\00\01z\00\03\00\04zxcv\00\01c\00\02vv\00\05last.\00\03\00\03one\00\03two\00\05three"
    )
    assert codecs.decode_string_multimap(body) == {
        "as": ["df", "zxc"],
        "z": ["zxcv", "c", "vv"],
        "last.": ["one", "two", "three"],
    }


def test_codecs_decode_consistency_good():
    body = SBytes(b"\x00\x04")
    obj = codecs.decode_consistency(body)
    assert obj == Consistency.QUORUM


def test_codecs_decode_consistency_bad():
    with pytest.raises(InternalDriverError, match=r"unknown consistency=abba"):
        body = SBytes(b"\xab\xba")
        codecs.decode_consistency(body)


def test_codecs_decode_byte_good():
    body = SBytes(b"\xe0\x04")
    value = codecs.decode_byte(body)
    assert value == 224 and body.remaining == b"\x04"


def test_codecs_decode_length_bytes_good():
    body = SBytes(b"\x01\x02\x03\x04")
    value = codecs.decode_length_bytes(body, 3)
    assert value == b"\x01\x02\x03" and body.remaining == b"\x04"


def test_codecs_decode_length_bytes_zero():
    body = SBytes(b"\x01\x02\x03\x04")
    value = codecs.decode_length_bytes(body, 0)
    assert value == b"" and body.remaining == b"\x01\x02\x03\x04"
