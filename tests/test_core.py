import pytest

from pysandra.core import SBytes, Streams, pretty_type
from pysandra.exceptions import InternalDriverError, MaximumStreamsException


def test_pretty_type_int():
    assert pretty_type(1) == "int"


def test_pretty_type_str():
    assert pretty_type("1") == "str"


def test_pretty_type_float():
    assert pretty_type(1.2) == "<class 'float'>"


def test_max_streams():
    with pytest.raises(
        MaximumStreamsException, match=r"too many streams last_id=31159 length=32769"
    ):
        streams: Streams[int] = Streams()
        move = 0
        while True:
            move += 1
            stream_id = streams.create()
            streams.update(stream_id, 3)
            if (move % 19) == 0:
                streams.remove(stream_id)


def test_streams_list():
    streams: Streams[int] = Streams()
    streams.create()
    streams.create()
    assert streams.items() == [0, 1]


def test_streams_update():
    streams: Streams[int] = Streams()
    stream_id = streams.create()
    stream_id2 = streams.create()
    streams.update(stream_id, "FOO")
    streams.update(stream_id2, "BAR")
    streams.remove(stream_id2)
    assert streams.remove(stream_id) == "FOO"


def test_streams_update_fail_found():
    with pytest.raises(InternalDriverError, match=r"not being tracked"):
        streams: Streams[int] = Streams()
        stream_id = streams.create()
        streams.update(stream_id + 1, "FOO")


def test_streams_update_fail_null():
    with pytest.raises(InternalDriverError, match=r"empty request"):
        streams: Streams[int] = Streams()
        stream_id = streams.create()
        streams.update(stream_id, None)


def test_streams_error():
    with pytest.raises(InternalDriverError, match=r"is not open"):
        streams: Streams[int] = Streams()
        stream_id = streams.create()
        streams.remove(stream_id + 1)


def test_sbytes_at_end():
    t = SBytes(b"12345")
    print(f"{t.grab(1)!r}{t.at_end()}")
    print(f"{t.grab(3)!r}{t.at_end()}")
    print(f"{t.grab(1)!r}{t.at_end()}")
    assert t.at_end()


def test_sbytes_hex():
    t = SBytes(b"\x03\13\45")
    assert t.hex() == "0x030b25"


def test_sbytes_remaining():
    t = SBytes(b"\x03\13\45")
    t.grab(2)

    assert t.remaining == b"%"


def test_sbytes_not_end():
    t = SBytes(b"12345")
    print(f"{t.grab(1)!r}{t.at_end()}")
    print(f"{t.grab(3)!r}{t.at_end()}")
    assert not t.at_end()


def test_sbytes_overflow():
    with pytest.raises(InternalDriverError, match=r"cannot go beyond"):
        t = SBytes(b"12345")
        print(f"{t.grab(1)!r}{t.at_end()}")
        print(f"{t.grab(3)!r}{t.at_end()}")
        print(f"{t.grab(2)!r}{t.at_end()}")
