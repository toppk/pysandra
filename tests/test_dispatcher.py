import ssl

from pysandra.dispatcher import Dispatcher


def test_dispatcher_simple():
    d = Dispatcher("blank", "host", 0, False)
    assert d._host == "host"


def test_dispatcher_tls():
    d = Dispatcher("blank", "host", 0, True)
    assert d._tls.verify_mode == ssl.CERT_NONE
