import pytest

from pysandra import exceptions, protocol
from pysandra.core import SBytes


def test_protocol_header_good():
    p = protocol.Protocol()
    p.version = 3
    stream = SBytes(b"\x83\x00\x00\x00\x00\x00\x00\x00\x01")
    data = p.decode_header(stream)
    assert data[4] == 1


def test_protocol_header_bad():
    with pytest.raises(
        exceptions.VersionMismatchException,
        match=r"received incorrect version from server",
    ):
        p = protocol.Protocol()
        p.version = 2
        stream = SBytes(b"\x83\x00\x00\x00\x00\x00\x00\x00\x01")
        p.decode_header(stream)


def test_protocol_baseclass_event_handler_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.event_handler(1, 1, 1, 1, 1, b"")


def test_protocol_baseclass_build_response_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.build_response(1, 1, 1, 1, 1, 1, 1)


def test_protocol_baseclass_startup_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.startup(1, 1)


def test_protocol_baseclass_query_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.query(1, 1)


def test_protocol_baseclass_execute_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.execute(1, 1)


def test_protocol_baseclass_prepare_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.prepare(1, 1)


def test_protocol_baseclass_options_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.options(1, 1)


def test_protocol_baseclass_register_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.register(1, 1)
