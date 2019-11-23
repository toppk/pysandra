import pytest

from pysandra.connection import Connection
from pysandra.exceptions import InternalDriverError


def test_connection_basic():
    conn = Connection()
    assert conn.host == "127.0.0.1"


def test_connection_choices():
    conn = Connection()
    conn.make_choices({"COMPRESSION": ["snappy", "foz"]})
    assert conn.options["COMPRESSION"] == "snappy"


def test_connection_choices_nomatch():
    conn = Connection()
    conn.make_choices({"COMPRESSION": ["sdf", "foz"]})
    assert "COMPRESSION" not in conn._options


def test_connection_choices_prefer():
    conn = Connection()
    conn.make_choices({"COMPRESSION": ["snappy", "lz4"]})
    assert conn._options["COMPRESSION"] == "lz4"


def test_connection_choices_comgood():
    conn = Connection()
    conn.make_choices({"COMPRESSION": ["snappy", "lz4"]})
    assert conn.compress(b"123") == b"\x00\x00\x00\x030123"


def test_connection_choices_combad():
    with pytest.raises(InternalDriverError, match=r"no compression selected"):
        conn = Connection()
        conn.make_choices({})
        conn.compress(b"123")


def test_connection_choices_decgood():
    conn = Connection()
    conn.make_choices({"COMPRESSION": ["snappy"]})
    assert (
        conn.decompress(b"\x15\x0crow \x11\x04 your boat") == b"row row row your boat"
    )


def test_connection_choices_decbad():
    with pytest.raises(InternalDriverError, match=r"no compression selected"):
        conn = Connection()
        conn.make_choices({"COMPRESSION": ["liver"]})
        conn.decompress(b"yes!")
