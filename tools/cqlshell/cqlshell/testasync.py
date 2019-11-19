#!/usr/bin/env python3

import argparse
import asyncio
import sys

from pysandra import Client


async def test_dml(client):
    query = "SELECT release_version FROM system.local"
    print(f"========> RUNNING {query}")
    rows = await client.execute(query)
    for row in rows:
        print(f"got row={row}")
    print(f"========> FINISHED")
    query = "SELECT * FROM uprofile.user where user_id=1"
    print(f"========> RUNNING {query}")
    rows = await client.execute("SELECT * FROM uprofile.user where user_id=1")
    for row in rows:
        print(f">>> got row={row}")
    print(f"========> FINISHED")
    prepare = (
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)"
    )
    print(f"========> PREPARING {prepare}")
    insert = await client.prepare(prepare)
    data = [45, "Trump", "Washington D.C."]
    print(f"========> INSERTING {data}")
    status = await client.execute(insert, data)
    print(f">>> got status={status}")
    print(f"========> FINISHED")
    query = "SELECT * FROM uprofile.user where user_id=45"
    print(f"========> RUNNING {query}")
    rows = await client.execute(query)
    for row in rows:
        print(f">>> got row={row}")
    print(f"========> FINISHED")


async def test_ddl(client):
    query = "CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    print(f"========> RUNNING {query}")
    status = await client.execute(query)
    print(f">>> got status={status}")
    print(f"========> FINISHED")
    query = "CREATE TABLE IF NOT EXISTS uprofile.user (user_id int , user_name text, user_bcity text, PRIMARY KEY( user_id, user_name))"
    print(f"========> RUNNING {query}")
    status = await client.execute(query)
    print(f">>> got status={status}")
    print(f"========> FINISHED")


async def test_dupddl(client):
    query = "CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    print(f"========> RUNNING {query}")
    status = await client.execute(query)
    print(f">>> got status={status}")
    print(f"========> FINISHED")
    # query=  "CREATE KEYSPACE testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    # print(f"========> RUNNING {query}")
    # status = await client.execute(query)
    # print(f">>> got status={status}")
    # print(f"========> FINISHED")


def die(exit_on_error):
    def finish(error_code):
        print(f"error_code={error_code} and exit_on_error={exit_on_error}")
        if error_code is not None and error_code != 0 and exit_on_error:
            print("will exist")
            sys.exit(error_code)

    return finish


async def run(command, stop=False):
    finish = die(stop)
    client = Client()
    await client.connect()
    if command in ("ddl", "full"):
        finish(await test_ddl(client))
    if command in ("dml", "full"):
        finish(await test_dml(client))
    if command in ("dupddl"):
        finish(await test_dupddl(client))
    await client.close()
    if command not in ("ddl", "dml", "full", "dupddl"):
        print(f"ERROR:unknown command={command}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--stop", dest="stop", action="store_true")
    parser.add_argument("--no-stop", dest="stop", action="store_false")
    parser.set_defaults(stop=False)
    args = parser.parse_args()
    asyncio.run(run(args.command, args.stop))
