from collections import OrderedDict
from ipaddress import IPv4Address

import pytest

from pysandra import types


def test_types_inet():
    o = types.InetType(1, 2)
    assert o.port == 2


def test_types_inet_eq_good():
    o = types.InetType(1, 2)
    b = types.InetType(1, 2)
    assert o == b


def test_types_inet_eq_bad():
    o = types.InetType(1, 2)
    assert o != 2


def test_types_inet_repr():
    o = types.InetType(IPv4Address("9.9.9.9"), 0x01BB)
    assert repr(o) == "InetType(ipaddr=IPv4Address('9.9.9.9'), port=443)"


def test_types_topologychange():
    o = types.TopologyChange(1, 2)
    assert o.node == 2


def test_types_rows():
    d = types.Rows()
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    assert list(d) == [(b"1", b"2"), (b"3", b"4")]


def test_types_rows_specs_cont():
    d = types.Rows(col_specs=[{"name": "one"}, {"name": "two"}])
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    assert list(d)[1].two == b"4"


def test_types_rows_specs_property():
    d = types.Rows(col_specs=[{"name": "one"}, {"name": "two"}])
    assert len(d.col_specs) == 2


def test_types_rows_specs_b4():
    d = types.Rows()
    d.col_specs = [{"name": "one"}, {"name": "two"}]
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    assert list(d)[1].two == b"4"


def test_types_rows_specs_a4():
    d = types.Rows()
    d.add_row((b"1", b"2"))
    d.col_specs = [{"name": "one"}, {"name": "two"}]
    d.add_row((b"3", b"4"))
    assert list(d)[0].two == b"2"


def test_types_rows_specs_len():
    d = types.Rows()
    d.add_row((b"1", b"2"))
    assert len(d) == 1


def test_types_rows_specs_offset():
    d = types.Rows()
    d.add_row((b"1", b"2"))
    assert d[0] == (b"1", b"2")


def test_types_rows_reset():
    d = types.Rows()
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    count = len(list(d)) + len(list(d))
    assert count == 4


def test_types_rows_specs_repr():
    d = types.Rows()
    d.add_row((b"1", b"2"))
    d.col_specs = [{"name": "one"}, {"name": "two"}]
    d.add_row((b"3", b"4"))
    assert f"{list(d)[0]}" == "Row(one=b'1', two=b'2')"


def test_types_pagingrows_specs_init():
    d = types.PagingRows(paging_state=b"1")
    assert d.paging_state == b"1"


@pytest.mark.asyncio
async def test_types_pagingrow_asynciter():
    d = types.PagingRows(paging_state=b"1")
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    rows = []
    async for row in d:
        rows.append(row)
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_types_pagingrow_bigasynciter():
    async def extend(state):
        if state == b"1":
            h = types.PagingRows(paging_state=b"2")
            h.add_row((b"1", b"2"))
            h.add_row((b"3", b"4"))
            return h

        g = types.Rows()
        g.add_row((b"5", b"6"))
        g.add_row((b"7", b"8"))
        return g

    d = types.PagingRows(paging_state=b"1")
    d.page_ = extend
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    rows = []
    async for row in d:
        rows.append(row)
    assert len(rows) == 6


def test_types_row_specs_dict():
    d = types.Row(1, 2, 3, fields_=["a", "b", "c"])
    assert dict(d.asdict_()) == {"a": 1, "b": 2, "c": 3}


def test_types_row_specs_iter():
    d = types.Row(1, 2, 3, fields_=["a", "b", "c"])
    cnt = 0
    for _ele in d:
        cnt += 1
    assert cnt == 3


def test_types_row_specs_item():
    d = types.Row(1, 2, 3, fields_=["a", "b", "c"])
    assert d[2] == 3


def test_types_row_specs_asdict_index():
    data = OrderedDict()
    data["a"] = 1
    data["b"] = 2
    data["c"] = 3

    d = types.Row(**data)
    assert d[2] == 3


def test_types_row_specs_asdict_attr():
    data = OrderedDict()
    data["a"] = 1
    data["b"] = 2
    data["c"] = 3

    d = types.Row(**data)
    assert d.b == 2


def test_types_row_specs_getattr():
    data = OrderedDict()
    data["a"] = 1
    data["b"] = 2
    data["c"] = 3

    d = types.Row(**data)
    assert getattr(d, "foo", None) is None


def test_types_row_specs_eq2():
    data = OrderedDict()
    data["a"] = 1
    data["b"] = 2
    data["c"] = 3

    d = types.Row(**data)
    g = types.Row(1, 2, 3, fields_=["a", "b", "c"])
    assert d == g


def test_types_row_specs_eq2_bad():
    from uuid import uuid4

    d = uuid4()
    g = types.Row(1, 2, 3, fields_=["a", "b", "c"])
    assert d != g


def test_types_row_specs_len():
    d = types.Row(b"1", b"2", fields_=["a", "b"])
    assert len(d) == 2


def test_types_row_specs_dir():
    d = types.Row(b"1", b"2", fields_=["a", "b"])
    assert "args" not in dir(d) and "a" in dir(d)


def test_types_schemachange():
    sc = types.SchemaChange(None, None, None)
    assert sc.options is None


def test_types_statuschange():
    sc = types.StatusChange(1, 2)
    assert sc.node == 2
