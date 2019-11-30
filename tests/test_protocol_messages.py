import pytest

from pysandra import constants, exceptions, protocol
from pysandra.constants import Consistency, Events, Opcode, SchemaChangeTarget
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


def test_protocol_messages_execute_alltypes():
    import datetime
    import decimal
    import ipaddress
    import uuid

    expected_body = (
        b"\x00\x10W\xa5g\xe7\xd3r'\xc1\x85\xf7\x06}<\xc3\xadp\x00\x01\x03\x00\x13\x00"
        + b"\x00\x00\x011\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02"
        + b"\x00\x00\x00\x02\x03\x06\x00\x00\x00\x01\x00\x00\x00\x00\x04\x80\x00G5\x00"
        + b"\x00\x00\t\x00\x00\x00\x08\r\xf9\x03C?\x00\x00\x00\x08@\x1c~M"
        + b"\xe3\xb8\xa1\x9d\x00\x00\x00\x04A\x05\x82\xe4\x00\x00\x00\x10&\x07\xf8\xb0"
        + b"@\x06\x08\x13\x00\x00\x00\x00\x00\x00 \x0e\x00\x00\x00\x04\x00\x00\x00\n"
        + b"\x00\x00\x00\x02\x00\x0b\x00\x00\x00\x0212\x00\x00\x00\x08\x00\x00\x00\x00"
        + b"\x00\x00\x00\r\x00\x00\x00\x08\x00\x00\x01n\xb8@\xa3\x1b\x00\x00\x00\x10"
        + b"v\x92\x80\xc8\x12\xf0\x11\xea\x88\x99`\xa4L\xe9tb\x00\x00\x00\x01"
        + b"\x10\x00\x00\x00\x10\xf9&0\xa6\xd9\x94D\x0e\xa2\xdc\xfek(\xe98)\x00\x00\x00"
        + b"\x0218\x00\x00\x00\x01\x13"
    )
    msg = protocol.ExecuteMessage(
        b"W\xa5g\xe7\xd3r'\xc1\x85\xf7\x06}<\xc3\xadp",
        [
            "1",
            2,
            b"\x03\x06",
            False,
            datetime.date(2019, 11, 29),
            decimal.Decimal("600.12315455"),
            7.123344,
            8.344455999,
            ipaddress.IPv6Address("2607:f8b0:4006:813::200e"),
            10,
            11,
            "12",
            13,
            datetime.datetime(2019, 11, 29, 17, 41, 14, 138904),
            uuid.UUID("769280c8-12f0-11ea-8899-60a44ce97462"),
            16,
            uuid.UUID("f92630a6-d994-440e-a2dc-fe6b28e93829"),
            "18",
            19,
        ],
        False,
        [
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myascii",
                "option_id": 1,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mybigint",
                "option_id": 2,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myblob",
                "option_id": 3,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myboolean",
                "option_id": 4,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mydate",
                "option_id": 17,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mydecimal",
                "option_id": 6,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mydouble",
                "option_id": 7,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myfloat",
                "option_id": 8,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myinet",
                "option_id": 16,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myint",
                "option_id": 9,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mysmallint",
                "option_id": 19,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mytext",
                "option_id": 13,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mytime",
                "option_id": 18,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mytimestamp",
                "option_id": 11,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mytimeuuid",
                "option_id": 15,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "mytinyint",
                "option_id": 20,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myuuid",
                "option_id": 12,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myvarchar",
                "option_id": 13,
            },
            {
                "ksname": "uprofile",
                "tablename": "alltypes",
                "name": "myvarint",
                "option_id": 14,
            },
        ],
        4,
        0,
        0,
        consistency=Consistency.ONE,
    )
    assert msg.encode_body() == expected_body
