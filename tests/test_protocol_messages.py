import pytest

from pysandra import constants, exceptions, protocol
from pysandra.constants import Events, Opcode, SchemaChangeTarget
from pysandra.core import SBytes


def test_protocol_message_response_create_bad():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"subclass should implement method"
    ):

        class MyResponse(protocol.ResponseMessage):
            pass

        MyResponse.create(1, 1, 1, SBytes(b""))


def test_protocol_message_response_create_emtpy():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"didn't generate a response message"
    ):

        class MyResponse(protocol.ResponseMessage):
            @staticmethod
            def build(version, flags, strem_id, body):
                return None

        MyResponse.create(1, 1, 1, SBytes(b""))


def test_protocol_message_response_create_remains():
    with pytest.raises(exceptions.InternalDriverError, match=r"left data remains"):

        class MyResponse(protocol.ResponseMessage):
            @staticmethod
            def build(version, flags, strem_id, body):
                return MyResponse(1, 2, 3)

        MyResponse.create(1, 1, 1, SBytes(b"asdf"))


def test_protocol_message_response_create_good():
    class MyResponse(protocol.ResponseMessage):
        @staticmethod
        def build(version, flags, strem_id, body):
            body.grab(4)
            return MyResponse(1, 2, 3)

    msg = MyResponse.create(1, 1, 1, SBytes(b"asdf"))
    assert msg.version == 1


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


def test_protocol_messages_errormsg_build_good():
    msg = protocol.ErrorMessage.build(
        1,
        2,
        3,
        SBytes(
            b'\x00\x00"\x00\x00;Invalid STRING constant (hillary) for "user_id" of type int'
        ),
    )
    assert msg.error_code == constants.ErrorCode.INVALID


def test_protocol_messages_errormsg_build_err():
    with pytest.raises(
        exceptions.InternalDriverError, match=r"unknown error_code=feebad"
    ):
        protocol.ErrorMessage.build(
            1, 2, 3, SBytes(b"\x00\xfe\xeb\xad\x00"),
        )


def test_protocol_messages_errormsg_build_unavailable():
    body = b"\x00\x00\x10\x00\x00&Cannot achieve consistency level THREE\x00\x03\x00\x00\x00\x03\x00\x00\x00\x01"
    msg = protocol.ErrorMessage.build(1, 2, 3, SBytes(body),)
    assert msg.error_code == constants.ErrorCode.UNAVAILABLE_EXCEPTION


def test_protocol_messages_event_build_good():
    body = b"\x00\rSCHEMA_CHANGE\x00\x07CREATED\x00\x08KEYSPACE\x00\x0ctestkeyspace"
    msg = protocol.EventMessage.build(1, 2, 3, SBytes(body),)
    assert msg.event_type == Events.SCHEMA_CHANGE


def test_protocol_messages_event_build_badevent():
    with pytest.raises(
        exceptions.UnknownPayloadException, match=r"got unexpected event=SCHEMA_change"
    ):
        body = b"\x00\rSCHEMA_change\x00\x07CREATED\x00\x08KEYSPACE\x00\x0ctestkeyspace"
        protocol.EventMessage.build(
            1, 2, 3, SBytes(body),
        )


def test_protocol_messages_event_build_badchange():
    with pytest.raises(
        exceptions.UnknownPayloadException, match=r"got unexpected change_type=CRE4TED"
    ):
        body = b"\x00\rSCHEMA_CHANGE\x00\x07CRE4TED\x00\x08KEYSPACE\x00\x0ctestkeyspace"
        protocol.EventMessage.build(
            1, 2, 3, SBytes(body),
        )


def test_protocol_messages_event_build_badtargete():
    with pytest.raises(
        exceptions.UnknownPayloadException, match=r"got unexpected target=K3YSPACE"
    ):
        body = b"\x00\rSCHEMA_CHANGE\x00\x07CREATED\x00\x08K3YSPACE\x00\x0ctestkeyspace"
        protocol.EventMessage.build(
            1, 2, 3, SBytes(body),
        )


def test_protocol_messages_event_build_good_table():
    body = (
        b"\x00\rSCHEMA_CHANGE\x00\x07CREATED\x00\x05TABLE\x00\x07mykeysp\x00\x07mytable"
    )
    msg = protocol.EventMessage.build(1, 2, 3, SBytes(body),)
    assert (
        msg.event.options["target_name"] == "mytable"
        and msg.event.target == SchemaChangeTarget.TABLE
    )


def test_protocol_messages_event_build_good_function():
    body = b"\x00\rSCHEMA_CHANGE\x00\x07CREATED\x00\x08FUNCTION\x00\x07mykeysp\x00\x07mytable\x00\x02\x00\x03cat\x00\x04book"
    msg = protocol.EventMessage.build(1, 2, 3, SBytes(body),)
    assert (
        msg.event.options["argument_types"] == ["cat", "book"]
        and msg.event.target == SchemaChangeTarget.FUNCTION
    )


def test_protocol_messages_preparedresults_meta():
    body = (
        b"\x00\x00\x00\x04\x00\x10\xac\xfc\x0fW\xa9\x9c\x1cr\xaf\xcaP9<\xd2c\x8d\x00\x00\x00"
        + b"\x01\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x08uprofile\x00\x04user\x00\x07user_id\x00"
        + b"\t\x00\tuser_name\x00\r\x00\nuser_bcity\x00\r\x00\x00\x00\x04\x00\x00\x00\x00"
    )
    msg = protocol.ResultMessage.build(1, 2, 3, SBytes(body),)
    assert msg.col_specs == [
        {"ksname": "uprofile", "name": "user_id", "option_id": 9, "tablename": "user"},
        {
            "ksname": "uprofile",
            "name": "user_name",
            "option_id": 13,
            "tablename": "user",
        },
        {
            "ksname": "uprofile",
            "name": "user_bcity",
            "option_id": 13,
            "tablename": "user",
        },
    ]


def test_protocol_messages_rowresults_global():
    body = (
        b"\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x08uprofile\x00\x04user"
        + b"\x00\x07user_id\x00\t\x00\tuser_name\x00\r\x00\nuser_bcity\x00\r\x00\x00\x00\x01"
        + b"\x00\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00\x06Ehtevs\x00\x00\x00\x04Pune"
    )
    msg = protocol.ResultMessage.build(1, 2, 3, SBytes(body),)
    assert msg.rows.col_specs == [
        {"ksname": "uprofile", "name": "user_id", "option_id": 9, "tablename": "user"},
        {
            "ksname": "uprofile",
            "name": "user_name",
            "option_id": 13,
            "tablename": "user",
        },
        {
            "ksname": "uprofile",
            "name": "user_bcity",
            "option_id": 13,
            "tablename": "user",
        },
    ]


def test_protocol_messages_rowresults_noglobal():
    body = (
        b"\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x08uprofile\x00\x04user\x00"
        + b"\x07user_id\x00\t\x00\tuser_name\x00\r\x00\nuser_bcity\x00\r\x00\x00\x00\x01\x00"
        + b"\x00\x00\x04\x00\x00\x00\x04\x00\x00\x00\x06Ehtevs\x00\x00\x00\x04Pune"
    )
    msg = protocol.ResultMessage.build(1, 2, 3, SBytes(body),)
    assert msg.rows.col_specs[0]["name"] == "user_id"


def test_protocol_messages_voidresults():
    body = b"\x00\x00\x00\x01"
    msg = protocol.ResultMessage.build(1, 2, 3, SBytes(body),)
    assert isinstance(msg, protocol.VoidResultMessage)


def test_protocol_message_setkeyspaceresult():
    body = b"\x00\x00\x00\x03\x00\x08uprofile"
    msg = protocol.ResultMessage.build(1, 2, 3, SBytes(body),)
    assert msg.keyspace == "uprofile"


def test_protocol_messages_register_bad():
    with pytest.raises(
        exceptions.TypeViolation,
        match=r"unknown event=asdf. please use pysandra.Events",
    ):
        msg = protocol.RegisterMessage(["asdf"], 1, 1, 1)
        msg.encode_body()
