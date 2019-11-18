#!/usr/bin/python3

import asyncio

from pysandra import Client


async def tcp_cassandra_client():

    client = Client()

    #await client.connect()

    status = await client.execute("CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1' }")
    print(f"got status={status}")
    status = await client.execute("CREATE TABLE IF NOT EXISTS uprofile.user (user_id int , user_name text, user_bcity text, PRIMARY KEY( user_id, user_name))");
    print(f"got status={status}")
    rows = await client.execute("SELECT release_version FROM system.local")
    for row in rows:
        print(f"got row={row}")
    rows = await client.execute("SELECT * FROM uprofile.user where user_id=1")
    for row in rows:
        print(f"got row={row}")

    insert = await client.prepare("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)")
    status = await client.execute(insert, [45,'Trump','Washington D.C.'])
    print(f"got status={status}")
    rows = await client.execute("SELECT * FROM uprofile.user where user_id=45")
    for row in rows:
        print(f"got row={row}")

    await client.close()
    

import argparse
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("account")
    args = parser.parse_args()
    #asyncio.run(tcp_finger_client(args.account))
    asyncio.run(tcp_cassandra_client())
