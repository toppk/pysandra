import os

import pytest

from pysandra.exceptions import InternalDriverError
from pysandra.utils import PKZip, fetch_module, get_logger, set_debug


@pytest.fixture
def pkzip():
    return PKZip()


def test_import_failure():
    assert fetch_module("xzcvzxcv") is None


def test_pkzip_lz4_supported(pkzip):
    algos = sorted(pkzip.supported)
    assert algos == ["lz4", "snappy"]


def test_pkzip_lz4_decompress(pkzip):
    algo = "lz4"
    cdata = b"\x00\x00\x00\x04@\x00\x00\x00\x01"
    data = pkzip.decompress(cdata, algo)
    assert data == b"\x00\x00\x00\x01"


def test_pkzip_lz4_compress(pkzip):
    algo = "lz4"
    data = b"\x00\x00\x00\x01"
    cdata = pkzip.compress(data, algo)
    assert cdata == b"\x00\x00\x00\x04@\x00\x00\x00\x01"


def test_pkzip_snappy_data(pkzip):
    data = b"asdfasdfa12311111111111234234dfasdfsdfasdfasdf"
    algo = "snappy"
    cdata = pkzip.compress(data, algo)
    assert cdata == b".\x0casdf\x05\x04\x0c1231\x19\x01\x14234234\t\x1b(sdfasdfasdf"


def test_pkzip_lz4_data(pkzip):
    data = b"asdfasdfa12311111111111234234dfasdfsdfasdfasdf"
    algo = "lz4"
    cdata = pkzip.compress(data, algo)
    assert (
        cdata
        == b"\x00\x00\x00.Aasdf\x04\x00F1231\x01\x00b234234\x1b\x00\xb0sdfasdfasdf"
    )


def test_pkzip_emtpty_lz4(pkzip):
    data = b""
    algo = "lz4"
    cdata = pkzip.compress(data, algo)
    assert cdata == b"\x00\x00\x00\x00\x00"


def test_pkzip_empty_snappy(pkzip):
    data = b""
    algo = "snappy"
    cdata = pkzip.compress(data, algo)
    assert cdata == b"\x00"


def test_pkzip_bad_algo_com(pkzip):
    with pytest.raises(InternalDriverError, match=r"not supported algo"):
        data = b""
        pkzip.compress(data, "foo")


def test_pkzip_bad_algo_dec(pkzip):
    with pytest.raises(InternalDriverError, match=r"not supported algo"):
        data = b""
        pkzip.decompress(data, "foo")


def test_logging_debugnver(caplog):
    logger = get_logger("pysandra.pytest_")
    string = "this is a long string!"
    logger.debug(string)
    assert string not in caplog.text


def test_logging_debugon(caplog):
    import pysandra.utils

    pysandra.utils._LOGGER_INITIALIZED = False
    os.environ["PYSANDRA_LOG_LEVEL"] = "DEBUG"
    logger = get_logger("pysandra.pytest_")
    string = "this is a long string!"
    logger.debug(string)
    assert string in caplog.text
    set_debug(False)


def test_logging_debugoff(caplog):
    import pysandra.utils

    pysandra.utils._LOGGER_INITIALIZED = False
    os.environ["PYSANDRA_LOG_LEVEL"] = "DEBUG"
    logger = get_logger("pysandra.pytest_")
    string = "this is a long string!"
    set_debug(False)
    logger.debug(string)
    assert string not in caplog.text


def test_logging_warning(caplog):
    logger = get_logger("pysandra.pytest_")
    string = "this is a long string!"
    logger.warning(string)
    assert string in caplog.text and "WARNING" in caplog.text
