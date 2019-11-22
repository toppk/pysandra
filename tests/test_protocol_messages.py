from pysandra import protocol


def test_encode_string():
    data = protocol.encode_string("test")
    assert data == b"\x00\x04test"
