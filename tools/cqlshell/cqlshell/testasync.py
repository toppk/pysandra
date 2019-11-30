#!/usr/bin/env python3

import argparse
import asyncio
import datetime
import decimal
import ipaddress
import sys
import uuid
from signal import Signals

import pysandra
from pysandra.utils import set_debug


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
        if isinstance(resp, pysandra.Rows):
            for row in resp:
                print(f"got row={row}")
        elif isinstance(resp, pysandra.SchemaChange):
            print(f">>> got schema_change={resp}")
        elif isinstance(resp, bool):
            print(f">>> got status={resp}")
        elif isinstance(resp, str):
            print(f">>> got state={resp}")
        else:
            raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")

    async def run_query(self, query, args=None, send_metadata=False):
        print(f"========> RUNNING {query} args={args} send_metadata={send_metadata}")
        resp = await self.client.execute(query, args, send_metadata=send_metadata)
        if isinstance(resp, pysandra.Rows):
            for row in resp:
                print(f"got row={row}")
        elif isinstance(resp, pysandra.SchemaChange):
            print(f">>> got schema_change={resp}")
        elif isinstance(resp, bool):
            print(f">>> got status={resp}")
        elif isinstance(resp, str):
            print(f">>> got state={resp}")
        else:
            raise ValueError(f"unexpected response={resp}")
        print(f"========> FINISHED")

    async def run_prepare(self, query, data, send_metadata=False, consistency=None):
        print(f"========> PREPARING {query}")
        statement_id = await self.client.prepare(query)
        for entry in data:
            print(f"========> EXECUTING {entry}")
            resp = await self.client.execute(
                statement_id,
                entry,
                send_metadata=send_metadata,
                consistency=consistency,
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
    except pysandra.ServerError as e:
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
    except pysandra.BadInputException as e:
        print(f">>> got BadInputException exception={e}")
        print(f"========> FINISHED")
    try:
        await tester.run_query(
            "INSERT INTO  uprofile.user (user_id, user_name , user_bcity) VALUES ('hillary', 2, 'DC')"
        )
    except pysandra.ServerError as e:
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
    await tester.run_query(
        "SELECT * FROM uprofile.user where user_id=45", send_metadata=True
    )
    await tester.run_query("DELETE FROM uprofile.user where user_id=45")
    await tester.run_query("SELECT * FROM uprofile.user where user_id=45")


async def test_sim(tester, port=None):
    await tester.close()
    print(f"port={port}")

    tester = Tester(pysandra.Client(host=("127.0.0.1", port), no_compress=True))
    await tester.connect()
    print(f"is connected = {tester.client.is_connected}")
    print(f"is ready = {tester.client.is_ready}")


async def test_tls(tester):
    await tester.close()
    # tester = Tester(Client(host=("127.0.0.1", 9042), use_tls=False, debug_signal=Signals.SIGUSR1))
    tester = Tester(
        pysandra.Client(
            host=("127.0.0.1", 9142), use_tls=True, debug_signal=Signals.SIGUSR1
        )
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


async def test_error(tester):
    await tester.run_prepare(
        "INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (:id,:n,:c)",
        [[45, "Trump", "Washington D.C."]],
        consistency=pysandra.Consistency.THREE,
    )


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


async def test_types(tester):
    # wheres counter
    await tester.run_query(
        "CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    await tester.run_query(
        "CREATE TABLE IF NOT EXISTS uprofile.alltypes (myascii ascii, mybigint bigint, myblob blob, myboolean boolean, "
        + "mydate date, mydecimal decimal, mydouble double, myfloat float, myinet inet, myint int, mysmallint smallint, "
        + "mytext text, mytime time, mytimestamp timestamp, mytimeuuid timeuuid, mytinyint tinyint, myuuid uuid, "
        + "myvarchar varchar, myvarint varint, PRIMARY KEY( myint))"
    )
    await tester.run_query(
        "CREATE TABLE IF NOT EXISTS uprofile.countertypes (myascii ascii, mybigint bigint,  mycounter1 counter, "
        + "mycounter2 counter, PRIMARY KEY(myascii, mybigint))"
    )
    await tester.run_prepare(
        "INSERT INTO  uprofile.alltypes  (myascii, mybigint, myblob, myboolean, mydate, mydecimal, mydouble, "
        + "myfloat, myinet, myint, mysmallint, mytext, mytime, mytimestamp, mytimeuuid, mytinyint, myuuid, "
        + "myvarchar, myvarint) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            [
                "1",
                2,
                b"\x03\x06",
                False,
                datetime.date(2019, 11, 29),
                decimal.Decimal("600.12315455"),  # fix
                7.123344,
                8.344455999,
                ipaddress.IPv6Address("2607:f8b0:4006:813::200e"),
                10,
                11,
                "12",
                13,
                datetime.datetime(2019, 11, 29, 17, 41, 14, 138904),
                uuid.UUID("769280c8-12f0-11ea-8899-60a44ce97462"),
                16,
                uuid.UUID("f92630a6-d994-440e-a2dc-fe6b28e93829"),
                "18",
                19,
            ]
        ],
        consistency=pysandra.Consistency.ONE,
    )
    await tester.run_query(" SELECT * FROM uprofile.alltypes", send_metadata=True)


async def test_ddl(tester):
    await tester.run_query(
        "CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    await tester.run_query(
        "CREATE TABLE IF NOT EXISTS uprofile.user (user_id int , user_name text, user_bcity text, PRIMARY KEY( user_id, user_name))"
    )


# in suite
async def test_events(tester):
    await tester.run_register([pysandra.Events.SCHEMA_CHANGE])


async def test_dupddl(tester):
    await tester.run_query(
        "CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
    )
    try:
        await tester.run_query(
            "CREATE KEYSPACE testkeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }"
        )
    except pysandra.ServerError as e:
        print(f">>> got ServerError exception={e.details}")
    await tester.run_query("DROP KEYSPACE testkeyspace")


async def run(command, stop=False, port=None):
    if command not in (
        "ddl",
        "error",
        "dml",
        "sim",
        "full",
        "dupddl",
        "ssl",
        "types",
        "events",
        "use",
        "bad",
        "meta",
        "dml2",
    ):
        print(f"ERROR:unknown command={command}")
        sys.exit(1)
    tester = Tester(pysandra.Client(debug_signal=Signals.SIGUSR1, no_compress=True))
    # await tester.connect()
    if command in ("ddl", "full",):
        await tester.connect()
        await test_ddl(tester)
    if command in ("dml", "full",):
        await tester.connect()
        await test_dml(tester)
    if command in ("meta", "full",):
        await tester.connect()
        await test_meta(tester)
    if command in ("bad",):
        await tester.connect()
        await test_bad(tester)
    if command in ("ssl",):
        await tester.connect()
        await test_tls(tester)
    if command in ("types",):
        await tester.connect()
        await test_types(tester)
    if command in ("sim",):
        await test_sim(tester, port=port)
    if command in ("dml2",):
        await tester.connect()
        await test_dml2(tester)
    if command in ("use",):
        await tester.connect()
        await test_use(tester)
    if command in ("error",):
        await tester.connect()
        await test_error(tester)
    if command in ("dupddl",):
        await tester.connect()
        await test_dupddl(tester)
    if command in ("events",):
        await tester.connect()
        await test_events(tester)

    await tester.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--port", "-p", dest="port")
    parser.add_argument("--stop", dest="stop", action="store_true")
    parser.add_argument("--no-stop", dest="stop", action="store_false")
    parser.add_argument("--debug", "-d", dest="debug", action="store_true")
    parser.set_defaults(stop=False)
    args = parser.parse_args()
    if args.debug:
        set_debug(True)
    asyncio.run(run(args.command, args.stop, port=args.port))
    print("finished")


if __name__ == "__main__":
    main()
