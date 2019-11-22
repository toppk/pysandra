import pysandra
import pytest


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_say():
    client = pysandra.Client()
    assert not client.is_connected()
    await client.connect()
    print(client.is_connected())
    assert client.is_connected()
    await client.close()
