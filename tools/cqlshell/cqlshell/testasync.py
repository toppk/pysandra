#!/usr/bin/env python3

import argparse
import asyncio
import sys
from signal import Signals

from pysandra import Client, Events, exceptions
from pysandra.types import Rows, SchemaChange
from pysandra.utils import enable_debug


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

    async def run_simple_query(self, query, send_metadata=False):
        print(f"========> RUNNING {query}")
        resp = await self.client.execute(query, send_metadata=send_metadata)
        if isinstance(resp, Rows):
            for row in resp:
                print(f"got row={row}")
        elif isinstance(resp, SchemaChange):
            print(f">>> got schema_change={resp}")
        elif isinstance(resp, bool):
            print(f">>> got status={resp}")
        elif isinstance(resp, str):
            print(f">>> got state={resp}")
        else:
            raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")

    async def run_query(self, query, args=None, send_metadata=False):
        print(f"========> RUNNING {query} args={args}")
        resp = await self.client.execute(query, args, send_metadata=send_metadata)
        if isinstance(resp, Rows):
            for row in resp:
                print(f"got row={row}")
        elif isinstance(resp, SchemaChange):
            print(f">>> got schema_change={resp}")
        elif isinstance(resp, bool):
            print(f">>> got status={resp}")
        elif isinstance(resp, str):
            print(f">>> got state={resp}")
        else:
            raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")

    async def run_prepare(self, query, data, send_metadata=False):
        print(f"========> PREPARING {query}")
        statement_id = await self.client.prepare(query)
        for entry in data:
            print(f"========> EXECUTING {entry}")
            resp = await self.client.execute(
                statement_id, entry, send_metadata=send_metadata
            )
            if isinstance(resp, bool):
                print(f">>> got status={resp}")
            else:
                raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")

    async def run_empty_prepare(self, query, count, send_metadata=False):
        print(f"========> PREPARING {query}")
        statement_id = await self.client.prepare(query)
        for _entry in range(count):
            print(f"========> INSERTING {_entry}")
            resp = await self.client.execute(statement_id, send_metadata=send_metadata)
            if isinstance(resp, bool):
                print(f">>> got status={resp}")
            else:
                raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")


# in suite
async def test_use(tester):
    try:
        await tester.run_query("SELECT * FROM user where user_id=1")
    except exceptions.ServerError as e:
        print(f">>> got ServerError exception={e.msg.error_text}")
        print(f"========> FINISHED")
    await tester.run_query("use uprofile")
    await tester.run_query("SELECT * FROM user where user_id=1")


# in suite
async def test_bad(tester):
    try:
        await tester.run_prepare(
            "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)",
            [["hillary", 2, "Washington D.C."]],
        )
    except exceptions.BadInputException as e:
        print(f">>> got BadInputException exception={e}")
        print(f"========> FINISHED")
    try:
        await tester.run_query(
            "INSERT INTO  uprofile.user (user_id, user_name , user_bcity) VALUES ('hillary', 2, 'DC')"
        )
    except exceptions.ServerError as e:
        print(f">>> got ServerError exception={e}")
        print(f"========> FINISHED")


async def test_meta(tester):
    await tester.run_query(
        "SELECT release_version FROM system.local", send_metadata=True
    )
    await tester.run_prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)",
        [[45, "Trump", "Washington D.C."]],
        send_metadata=True,
    )


async def test_dml(tester):
    await tester.run_query("SELECT release_version FROM system.local")
    await tester.run_query("SELECT * FROM uprofile.user where user_id=1")
    await tester.run_prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (:id,:n,:c)",
        [[45, "Trump", "Washington D.C."]],
    )
    await tester.run_query("SELECT * FROM uprofile.user where user_id=45")
    await tester.run_query("DELETE FROM uprofile.user where user_id=45")
    await tester.run_query("SELECT * FROM uprofile.user where user_id=45")


async def test_tls(tester):
    await tester.close()
    # tester = Tester(Client(host=("127.0.0.1", 9042), use_tls=False, debug_signal=Signals.SIGUSR1))
    tester = Tester(
        Client(host=("127.0.0.1", 9142), use_tls=True, debug_signal=Signals.SIGUSR1)
    )
    await tester.run_query("SELECT * FROM uprofile.user where user_id=?", (2,))
    await tester.run_query("SELECT * FROM uprofile.user where user_id=:id", {"id": 3})
    await tester.run_query("SELECT * FROM uprofile.user where user_id=:id", {"id": 45})
    await tester.run_empty_prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (45, 'Trump', 'Washington D.C.')",
        2,
    )
    await tester.run_query("SELECT * FROM uprofile.user where user_id=?", (45,))
    await tester.run_simple_query("DELETE FROM uprofile.user where user_id=45")
    await tester.close()


async def test_dml2(tester):
    await tester.run_query("SELECT * FROM uprofile.user where user_id=?", (2,))
    await tester.run_query("SELECT * FROM uprofile.user where user_id=:id", {"id": 3})
    await tester.run_query("SELECT * FROM uprofile.user where user_id=:id", {"id": 45})
    await tester.run_empty_prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (45, 'Trump', 'Washington D.C.')",
        2,
    )
    await tester.run_query("SELECT * FROM uprofile.user where user_id=?", (45,))
    await tester.run_simple_query("DELETE FROM uprofile.user where user_id=45")


async def test_ddl(tester):
    await tester.run_query(
        "CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    await tester.run_query(
        "CREATE TABLE IF NOT EXISTS uprofile.user (user_id int , user_name text, user_bcity text, PRIMARY KEY( user_id, user_name))"
    )


# in suite
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
    if command not in (
        "ddl",
        "dml",
        "full",
        "dupddl",
        "ssl",
        "events",
        "use",
        "bad",
        "meta",
        "dml2",
    ):
        print(f"ERROR:unknown command={command}")
        sys.exit(1)
    tester = Tester(Client(debug_signal=Signals.SIGUSR1))
    await tester.connect()
    if command in ("ddl", "full",):
        await test_ddl(tester)
    if command in ("dml", "full",):
        await test_dml(tester)
    if command in ("meta", "full",):
        await test_meta(tester)
    if command in ("bad",):
        await test_bad(tester)
    if command in ("ssl",):
        await test_tls(tester)
    if command in ("dml2",):
        await test_dml2(tester)
    if command in ("use",):
        await test_use(tester)
    if command in ("dupddl",):
        await test_dupddl(tester)
    if command in ("events",):
        await test_events(tester)

    await tester.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--stop", dest="stop", action="store_true")
    parser.add_argument("--no-stop", dest="stop", action="store_false")
    parser.add_argument("--debug", "-d", dest="debug", action="store_true")
    parser.set_defaults(stop=False)
    args = parser.parse_args()
    if args.debug:
        enable_debug()
    asyncio.run(run(args.command, args.stop))
    print("finished")


if __name__ == "__main__":
    main()
