import pytest

from pysandra import constants, exceptions, protocol
from pysandra.constants import Opcode
from pysandra.core import SBytes


def test_protocol_message_header_good():
    p = protocol.Protocol()
    p.version = 3
    stream = SBytes(b"\x83\x00\x00\x00\x00\x00\x00\x00\x01")
    data = p.decode_header(stream)
    assert data[4] == 1


def test_protocol_message_header_bad():
    with pytest.raises(
        exceptions.VersionMismatchException,
        match=r"received incorrect version from server",
    ):
        p = protocol.Protocol()
        p.version = 2
        stream = SBytes(b"\x83\x00\x00\x00\x00\x00\x00\x00\x01")
        p.decode_header(stream)


def test_protocol_messages_baseclass_event_handler_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.event_handler(1, 1, 1, 1, 1, b"")


def test_protocol_messages_baseclass_build_response_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.build_response(1, 1, 1, 1, 1, 1, 1)


def test_protocol_messages_baseclass_startup_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.startup(1, 1)


def test_protocol_messages_baseclass_query_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.query(1, 1)


def test_protocol_messages_baseclass_execute_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.execute(1, 1)


def test_protocol_messages_baseclass_prepare_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.prepare(1, 1)


def test_protocol_messages_baseclass_options_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.options(1, 1)


def test_protocol_messages_baseclass_register_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        p = protocol.Protocol()
        p.register(1, 1)


def test_protocol_messages_requestmessage_basic():
    msg = protocol.RequestMessage(1, 2, 3, 4)
    msg.opcode = 1
    assert bytes(msg) == b"\x01\x02\x00\x03\x01\x00\x00\x00\x00"


def test_protocol_messages_requestmessage_compress(monkeypatch):
    monkeypatch.setattr(protocol, "COMPRESS_MINIMUM", 20)
    msg = protocol.RequestMessage(1, 2, 3, lambda x: x[1:20])

    def encode_body(*args):
        return b"row row row your boat, gently down the stream" * 2

    msg.encode_body = encode_body
    msg.opcode = 1
    assert bytes(msg) == b"\x01\x03\x00\x03\x01\x00\x00\x00\x13ow row row your boa"


def test_protocol_messages_responsemessage_basic():
    msg = protocol.ResponseMessage(1, 2, 3)
    assert msg.flags == 2


def test_protocol_messages_responsemsg_build_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):
        msg = protocol.ResponseMessage(1, 2, 3)
        msg.build(1, 2, 3, b"123")


def test_protocol_messages_readymsg_build():
    msg = protocol.ReadyMessage.build(1, 2, 3, b"")
    assert msg.opcode == Opcode.READY


def test_protocol_messages_supportedmsg_build():
    msg = protocol.SupportedMessage.build(
        1, 2, 3, SBytes(b"\x00\x01\x00\x01a\x00\x02\x00\x01b\x00\x01c")
    )
    assert msg.options["a"] == ["b", "c"]


def test_protocol_messages_errormsg_build():
    msg = protocol.ErrorMessage.build(
        1,
        2,
        3,
        SBytes(
            b'\x00\x00"\x00\x00;Invalid STRING constant (hillary) for "user_id" of type int'
        ),
    )
    assert msg.error_code == constants.ErrorCode.INVALID


def test_protocol_messages_errormsg_build_unavailable():
    body = b"\x00\x00\x10\x00\x00&Cannot achieve consistency level THREE\x00\x03\x00\x00\x00\x03\x00\x00\x00\x01"
    msg = protocol.ErrorMessage.build(1, 2, 3, SBytes(body),)
    assert msg.error_code == constants.ErrorCode.UNAVAILABLE_EXCEPTION


def test_protocol_messages_event_build():
    pass


def test_protocol_messages_result_build():
    pass
