import pytest


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_disconnected(client):
    assert not client.is_connected()


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_connected(client):
    await client.connect()
    assert client.is_connected()


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query(client):
    query = "SELECT release_version FROM system.local"
    resp = await client.execute(query)

    assert list(resp)[0][0] == b"3.11.5"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_with_args(client):
    query = "SELECT * FROM uprofile.user where user_id=?"
    resp = await client.execute(query, (2,))

    assert list(resp)[0][2] == b"Dubai"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_with_namedargs(client):
    query = "SELECT * FROM uprofile.user where user_id=:id"
    resp = await client.execute(query, {"id": 2})

    assert list(resp)[0][2] == b"Dubai"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_with_filtering(client):
    query = "SELECT * FROM uprofile.user where user_bcity='Dubai' ALLOW FILTERING"
    resp = await client.execute(query)

    assert list(resp)[0][0] == b"\x00\x00\x00\x02"
