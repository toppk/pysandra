from signal import Signals

import pytest

from pysandra.client import Client, online
from pysandra.exceptions import TypeViolation


@pytest.mark.asyncio
async def test_client_online():
    class Test:
        flag = 3

        @online
        async def test(self, offset):
            return self.flag + offset

        async def connect(self):
            self.flag = 2

    t = Test()
    assert await t.test(2) == 4


def test_client_simple():
    c = Client()
    assert not c.is_connected


def test_client_debug_str_good(caplog):
    import os

    Client(debug_signal="SIGUSR1")
    os.kill(os.getpid(), 10)
    assert "Dumping Internal State" in caplog.text


def test_client_debug_int_good(caplog):
    import os

    Client(debug_signal=1)
    os.kill(os.getpid(), 1)
    assert "Dumping Internal State" in caplog.text


def test_client_debug_str_bad(caplog):
    with pytest.raises(TypeViolation, match=r"debug signal is not valid"):
        Client(debug_signal="SIGUSR2f")


def test_client_debug_int_bad(caplog):
    with pytest.raises(TypeViolation, match=r"debug signal is not valid"):
        Client(debug_signal=333)


def test_client_debug_sig(caplog):
    import os

    Client(debug_signal=Signals.SIGUSR2)
    os.kill(os.getpid(), 12)
    assert "Dumping Internal State" in caplog.text
