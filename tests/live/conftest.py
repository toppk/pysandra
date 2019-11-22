import asyncio

import pytest

from pysandra import Client


# needed for 3.6 compatability
def run_loop(func):
    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(func)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()


async def setup_db():
    # should make sure database has test data
    c = Client()
    await c.connect()
    print(f"in setup_db client={c.is_connected()}")
    await c.close()


@pytest.fixture(scope="session", autouse=True)
def session_scope():
    # needed for 3.6 compatability
    run_loop(setup_db())
    # asyncio.run(setup_db())


@pytest.fixture
async def client():
    c = Client()
    yield c
    if c.is_connected():
        await c.close()
