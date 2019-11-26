from pysandra import types


def test_types_inet():
    o = types.InetType(1, 2)
    assert o.port == 2


def test_types_topologychange():
    o = types.TopologyChange(1, 2)
    assert o.node == 2


def test_types_rows():
    d = types.Rows(columns_count=2)
    d.add(b"1")
    d.add(b"2")
    d.add(b"3")
    d.add(b"4")
    assert list(d) == [[b"1", b"2"], [b"3", b"4"]]


def test_types_rows_reset():
    d = types.Rows(columns_count=2)
    d.add(b"1")
    d.add(b"2")
    d.add(b"3")
    d.add(b"4")
    count = len(list(d)) + len(list(d))
    assert count == 4


def test_types_schemachange():
    sc = types.SchemaChange(None, None, None)
    assert sc.options is None


def test_types_statuschange():
    sc = types.StatusChange(1, 2)
    assert sc.node == 2
