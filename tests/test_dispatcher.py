import asyncio
from unittest.mock import Mock

import pytest

from pysandra.core import SBytes
from pysandra.dispatcher import Dispatcher
from pysandra.protocol import Protocol


def get_mock_coro(return_value):
    # @asyncio.coroutine
    async def mock_coro(*args, **kwargs):
        return return_value(*args, **kwargs)

    return Mock(wraps=mock_coro)


def test_dispatcher_simple():
    d = Dispatcher("blank", None, None)
    assert d._proto == "blank"


@pytest.mark.asyncio
async def test_dispatcher_send():
    mock_writer = Mock(spec=asyncio.StreamWriter)
    d = Dispatcher("blank", None, mock_writer)
    await d.send(lambda x, y: bytes([x]) + b" is stream_id", {})
    mock_writer.write.assert_called_with(b"\x00 is stream_id")


@pytest.mark.asyncio
async def test_dispatcher_receive():
    mock_reader = Mock(spec=asyncio.StreamReader)
    mock_proto = Mock(spec=Protocol)
    d = Dispatcher(mock_proto, mock_reader, None)
    d._reader = mock_reader
    stream = SBytes(b"\x85\x00\x00\x00\x00\x02\x01\x00\x02\xaa\xbb")
    mock_reader.read = get_mock_coro(lambda x: stream.grab(x))
    # mock_reader.returns = b'\01\84\00\00\08\00'
    mock_proto.decode_header.return_value = [1, 2, 0, 2, 2]
    data = await d._receive()
    assert data[5] == b"\xaa\xbb"
