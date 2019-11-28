from pysandra import types


def test_types_inet():
    o = types.InetType(1, 2)
    assert o.port == 2


def test_types_topologychange():
    o = types.TopologyChange(1, 2)
    assert o.node == 2


def test_types_rows():
    d = types.Rows(2)
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    assert list(d) == [(b"1", b"2"), (b"3", b"4")]


def test_types_rows_specs_cont():
    d = types.Rows(2, col_specs=[{"name": "one"}, {"name": "two"}])
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    assert list(d)[1].two == b"4"


def test_types_rows_specs_b4():
    d = types.Rows(2)
    d.col_specs = [{"name": "one"}, {"name": "two"}]
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    assert list(d)[1].two == b"4"


def test_types_rows_specs_a4():
    d = types.Rows(2)
    d.add_row((b"1", b"2"))
    d.col_specs = [{"name": "one"}, {"name": "two"}]
    d.add_row((b"3", b"4"))
    assert list(d)[0].two == b"2"


def test_types_rows_specs_len():
    d = types.Rows(2)
    d.add_row((b"1", b"2"))
    assert len(d) == 1


def test_types_rows_specs_offset():
    d = types.Rows(2)
    d.add_row((b"1", b"2"))
    assert d[0] == (b"1", b"2")


def test_types_rows_reset():
    d = types.Rows(2)
    d.add_row((b"1", b"2"))
    d.add_row((b"3", b"4"))
    count = len(list(d)) + len(list(d))
    assert count == 4


def test_types_rows_specs_repr():
    d = types.Rows(2)
    d.add_row((b"1", b"2"))
    d.col_specs = [{"name": "one"}, {"name": "two"}]
    d.add_row((b"3", b"4"))
    assert f"{list(d)[0]}" == "Row(one=b'1', two=b'2')"


def test_types_row_specs_dict():
    d = types.Row(1, 2, 3, fields=["a", "b", "c"])
    assert dict(d.asdict_()) == {"a": 1, "b": 2, "c": 3}


def test_types_row_specs_iter():
    d = types.Row(1, 2, 3, fields=["a", "b", "c"])
    cnt = 0
    for _ele in d:
        cnt += 1
    assert cnt == 3


def test_types_row_specs_item():
    d = types.Row(1, 2, 3, fields=["a", "b", "c"])
    assert d[2] == 3


def test_types_row_specs_len():
    d = types.Row(b"1", b"2", fields=["a", "b"])
    assert len(d) == 2


def test_types_row_specs_dir():
    d = types.Row(b"1", b"2", fields=["a", "b"])
    assert "args" not in dir(d) and "a" in dir(d)


def test_types_schemachange():
    sc = types.SchemaChange(None, None, None)
    assert sc.options is None


def test_types_statuschange():
    sc = types.StatusChange(1, 2)
    assert sc.node == 2
