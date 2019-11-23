from pysandra.dispatcher import Dispatcher


def test_max_streams():
    d = Dispatcher("blank", "", False, 0)
    print(f"({d})")
