import asyncio

import pytest

from pysandra import Client


class TidyClient(Client):
    _should_reset = False

    @property
    def reset(self):
        return self._should_reset

    @reset.setter
    def reset(self, value):
        self._should_reset = value

    async def reset_now(self):
        await setup_db(self)
        self._should_reset = False


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


async def setup_db(client=None, close=False):
    # should make sure database has test data
    if client is None or not client.is_connected():
        client = Client()
    await client.execute("DROP TABLE IF EXISTS uprofile.user")
    await client.execute("DROP KEYSPACE IF EXISTS uprofile")
    await client.execute(
        "CREATE KEYSPACE uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    await client.execute(
        "CREATE TABLE uprofile.user (user_id int , user_name text, user_bcity text, PRIMARY KEY( user_id, user_name))"
    )
    insert_data = await client.prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
    )
    await client.execute(insert_data, [1, "Lybkov", "Seattle"])
    await client.execute(insert_data, [2, "Doniv", "Dubai"])
    await client.execute(insert_data, [3, "Keviv", "Chennai"])
    await client.execute(insert_data, [4, "Ehtevs", "Pune"])
    await client.execute(insert_data, [5, "Dnivog", "Belgaum"])
    print(f"in setup_db client={client.is_connected()}")
    if close:
        await client.close()


@pytest.fixture(scope="session", autouse=True)
def session_scope():
    # needed for 3.6 compatability
    run_loop(setup_db(close=True))
    # asyncio.run(setup_db())


@pytest.fixture
async def client():
    c = TidyClient()
    yield c
    if c.reset:
        await c.reset_now()
    if c.is_connected():
        await c.close()
