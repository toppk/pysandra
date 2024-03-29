import pytest

from pysandra import BadInputException, Consistency, Events, ServerError


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_disconnected(client):
    assert not client.is_connected


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_connected(client):
    await client.connect()
    assert client.is_connected


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query(client):
    query = "SELECT release_version FROM system.local"
    resp = await client.execute(query, send_metadata=False)

    assert list(resp)[0][0] == b"3.11.5"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_with_args(client):
    query = "SELECT * FROM uprofile.user where user_id=?"
    resp = await client.execute(query, (2,), send_metadata=False)

    assert list(resp)[0][2] == b"Dubai"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_with_namedargs(client):
    query = "SELECT * FROM uprofile.user where user_id=:id"
    resp = await client.execute(query, {"id": 2}, send_metadata=False)

    assert list(resp)[0][2] == b"Dubai"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_with_filtering(client):
    query = "SELECT * FROM uprofile.user where user_bcity='Dubai' ALLOW FILTERING"
    resp = await client.execute(query, send_metadata=False)

    assert list(resp)[0][0] == b"\x00\x00\x00\x02"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_needs_filtering(client):
    with pytest.raises(
        ServerError, match=r"error_code=0x2200.*might involve data filtering"
    ):
        query = "SELECT * FROM uprofile.user where user_bcity='Dubai'"
        await client.execute(query, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_no_keyspace(client):
    with pytest.raises(
        ServerError, match=r"error_code=0x2200.*No keyspace has been specified"
    ):
        query = "SELECT * FROM user where user_id=3"
        await client.execute(query, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_query_use_keyspace(client):
    query = "SELECT * FROM user where user_id=3"
    await client.execute("use uprofile", send_metadata=False)
    resp = await client.execute(query, send_metadata=False)
    assert list(resp)[0][2] == b"Chennai"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_bad_prepare(client):
    with pytest.raises(BadInputException, match=r"expected type=int but got type=str"):
        prepare = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
        stmt_id = await client.prepare(prepare)
        data = ["hillary", 2, "Washington D.C."]
        await client.execute(stmt_id, data, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_bad_prepare_str(client):
    with pytest.raises(BadInputException, match=r"expected type=int but got type=str"):
        prepare = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
        stmt_id = await client.prepare(prepare)
        data = ["hillary", 2, "Washington D.C."]
        await client.execute(stmt_id, data, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_bad_prepare_int(client):
    with pytest.raises(BadInputException, match=r"expected type=str but got type=int"):
        prepare = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
        stmt_id = await client.prepare(prepare)
        data = [4, 2, "Washington D.C."]
        await client.execute(stmt_id, data, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_bad_data_query_server(client):
    with pytest.raises(
        ServerError, match=r"error_code=0x2200.*Invalid STRING constant.*of type int"
    ):
        query = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES ('hillary', 2, 'DC')"
        await client.execute(query, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_bad_data_query_bound(client):
    with pytest.raises(
        ServerError, match=r"error_code=0x2200.*Expected 4 or 0 byte int"
    ):
        query = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
        data = ("hillary", "not", "DC")
        await client.execute(query, data, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_simple_bad_data_query_namedbound(client):
    with pytest.raises(
        ServerError, match=r"error_code=0x2200.*Expected 4 or 0 byte int"
    ):
        query = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (:id,:name,:city)"
        data = {"name": 666, "id": "hillary", "city": "DC"}
        await client.execute(query, data, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_query(client):
    query = "SELECT release_version FROM system.local"
    resp = await client.execute(query, send_metadata=True)
    assert len(resp.col_specs) == 1


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_query_none(client):
    query = "SELECT release_version FROM system.local"
    resp = await client.execute(query, send_metadata=False)
    assert resp.col_specs is None


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_query_none_false(client):
    query = "SELECT release_version FROM system.local"
    resp = await client.execute(query, send_metadata=False)
    assert resp.col_specs is None


@pytest.mark.xfail(reason="broken api")
@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_prepared_nouse(client):
    client.reset = True
    prepare = (
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
    )
    stmt_id = await client.prepare(prepare)
    data = [45, "Trump", "Washington D.C."]
    resp = await client.execute(stmt_id, data, send_metadata=True)
    assert resp.col_specs is None


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_query_use(client):
    client.reset = True
    query = "use uprofile"
    await client.execute(query, send_metadata=False)
    query = "select * from user where user_id=4"
    resp = await client.execute(query, send_metadata=True)
    assert resp.col_specs[0]["name"] == "user_id"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_query_nouse(client):
    query = "select user_id from uprofile.user where user_id=4"
    resp = await client.execute(query, send_metadata=True)
    assert resp.col_specs[0]["name"] == "user_id"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_meta_schema_events(client):
    queue = await client.register([Events.SCHEMA_CHANGE])
    query = "CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    await client.execute(query, send_metadata=False)
    query = "DROP KEYSPACE testkeyspace"
    await client.execute(query, send_metadata=False)
    assert queue.get_nowait().options["target_name"] == "testkeyspace"


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_results_error_unavailable(client):
    with pytest.raises(
        ServerError,
        match=r"received error_code=0x1000.*Cannot achieve consistency level THREE",
    ):
        query = "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (45, 'Trump', 'Washington D.C.')"
        await client.execute(query, consistency=Consistency.THREE, send_metadata=False)


@pytest.mark.live
@pytest.mark.live_simple
@pytest.mark.asyncio
async def test_live_simple_use_keyspace(client):
    query = "use uprofile"
    resp = await client.execute(
        query, consistency=Consistency.THREE, send_metadata=False
    )
    assert resp == "uprofile"
