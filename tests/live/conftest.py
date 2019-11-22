import pytest


@pytest.fixture
async def client():
    from pysandra import Client

    c = Client()
    yield c
    if c.is_connected():
        await c.close()
