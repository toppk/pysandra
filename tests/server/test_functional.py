import pytest

from pysandra import Client


@pytest.mark.server
@pytest.mark.asyncio
async def test_server_functional_connect(server):
    # client = Client(('127.0.0.1', 37891), no_compress=True)
    client = Client(server.addr, no_compress=True)
    # client = Client()
    await client.connect()
    assert client.is_connected
    await client.close()
