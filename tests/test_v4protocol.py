import pytest

from pysandra.constants import Opcode
from pysandra.exceptions import InternalDriverError
from pysandra.messages import PreparedResultMessage, PrepareMessage
from pysandra.v4protocol import V4Protocol


def test_v4protocol_basic():
    v4 = V4Protocol()
    assert v4.compress is None


# the message are more fully tested in test_protocol_messages


def test_v4protocol_options():
    v4 = V4Protocol()
    params = {}
    assert v4.options(1, params).opcode == Opcode.OPTIONS


def test_v4protocol_startup():
    v4 = V4Protocol()
    params = {"options": {}}
    assert v4.startup(1, params).opcode == Opcode.STARTUP


def test_v4protocol_register():
    v4 = V4Protocol()
    params = {"events": []}
    assert v4.register(1, params).opcode == Opcode.REGISTER


def test_v4protocol_prepare():
    v4 = V4Protocol()
    params = {"query": ""}
    assert v4.prepare(1, params).opcode == Opcode.PREPARE


def test_v4protocol_execute_failed():
    with pytest.raises(
        InternalDriverError, match=r"missing statement_id=.*in prepared statements"
    ):
        v4 = V4Protocol()
        params = {"statement_id": b"1"}
        assert v4.execute(1, params).opcode == Opcode.EXECUTE


def test_v4protocol_execute_success():
    v4 = V4Protocol()
    stmt_id = b"uncommon"
    prepare = PrepareMessage("", 0, 0, 0)
    result = PreparedResultMessage(stmt_id, [], [], 0, 0, 0, 0)
    v4.respond(prepare, result)
    params = {"statement_id": stmt_id, "query_params": None, "send_metadata": False}
    assert v4.execute(1, params).opcode == Opcode.EXECUTE


def test_v4protocol_query():
    v4 = V4Protocol()
    params = {"query": "", "query_params": None, "send_metadata": False}
    assert v4.query(1, params).opcode == Opcode.QUERY
