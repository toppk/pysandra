from pysandra.types import Rows, SchemaChange


def test_rows():
    d = Rows(columns_count=2)
    d.add(b"1")
    d.add(b"2")
    d.add(b"3")
    d.add(b"4")
    assert list(d) == [[b"1", b"2"], [b"3", b"4"]]


def test_rows_reset():
    d = Rows(columns_count=2)
    d.add(b"1")
    d.add(b"2")
    d.add(b"3")
    d.add(b"4")
    count = len(list(d)) + len(list(d))
    assert count == 4


def test_schemachange():
    sc = SchemaChange(None, None, None)
    assert sc.options is None
