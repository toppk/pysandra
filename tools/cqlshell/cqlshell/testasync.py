#!/usr/bin/env python3

import argparse
import asyncio
import sys
from signal import Signals

from pysandra import Client, Events, exceptions
from pysandra.types import Rows, SchemaChange


class Tester:
    def __init__(self, client):
        self.client = client

    async def connect(self):
        return await self.client.connect()

    async def close(self):
        return await self.client.close()

    async def run_register(self, events):
        print(f"========> REGISTERING {events}")
        resp = await self.client.register(events)
        if isinstance(resp, asyncio.Queue):
            print(f"========> LISTENING for events")
            while True:
                data = await resp.get()
                print(f">>> got {data}")
        # will never end
        print(f"========> FINISHED")

    async def run_query(self, query):
        print(f"========> RUNNING {query}")
        resp = await self.client.execute(query)
        if isinstance(resp, Rows):
            for row in resp:
                print(f"got row={row}")
        elif isinstance(resp, SchemaChange):
            print(f">>> got schema_change={resp}")
        elif isinstance(resp, bool):
            print(f">>> got status={resp}")
        else:
            raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")

    async def run_prepare(self, query, data):
        print(f"========> PREPARING {query}")
        statement_id = await self.client.prepare(query)
        for entry in data:
            print(f"========> INSERTING {entry}")
            resp = await self.client.execute(statement_id, entry)
            if isinstance(resp, bool):
                print(f">>> got status={resp}")
            else:
                raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")


async def test_dml(tester):
    await tester.run_query("SELECT release_version FROM system.local")
    await tester.run_query("SELECT * FROM uprofile.user where user_id=1")
    await tester.run_prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)",
        [[45, "Trump", "Washington D.C."]],
    )
    await tester.run_query("SELECT * FROM uprofile.user where user_id=45")
    await tester.run_query("DELETE FROM uprofile.user where user_id=45")
    await tester.run_query("SELECT * FROM uprofile.user where user_id=45")


async def test_ddl(tester):
    await tester.run_query(
        "CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    await tester.run_query(
        "CREATE TABLE IF NOT EXISTS uprofile.user (user_id int , user_name text, user_bcity text, PRIMARY KEY( user_id, user_name))"
    )


async def test_events(tester):
    await tester.run_register([Events.SCHEMA_CHANGE])


async def test_dupddl(tester):
    await tester.run_query(
        "CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    try:
        await tester.run_query(
            "CREATE KEYSPACE testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
        )
    except exceptions.ServerError as e:
        print(f">>> got ServerError exception={e.msg.details}")
    await tester.run_query("DROP KEYSPACE testkeyspace")


async def run(command, stop=False):
    if command not in ("ddl", "dml", "full", "dupddl", "events"):
        print(f"ERROR:unknown command={command}")
        sys.exit(1)
    tester = Tester(Client(debug_signal=Signals.SIGUSR1))
    await tester.connect()
    if command in ("ddl", "full",):
        await test_ddl(tester)
    if command in ("dml", "full",):
        await test_dml(tester)
    if command in ("dupddl",):
        await test_dupddl(tester)
    if command in ("events",):
        await test_events(tester)
    await tester.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--stop", dest="stop", action="store_true")
    parser.add_argument("--no-stop", dest="stop", action="store_false")
    parser.set_defaults(stop=False)
    args = parser.parse_args()
    asyncio.run(run(args.command, args.stop))
    print("finished")
