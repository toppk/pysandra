#!/usr/bin/python3

import asyncio

from pysandra.client import Client


async def tcp_cassandra_client():

    client = Client()

    await client.connect()

    rows = await client.query("SELECT * FROM uprofile.user where user_id=1")
    for row in rows:
        print("got %s" % row)

    await client.close()
    

import argparse
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("account")
    args = parser.parse_args()
    #asyncio.run(tcp_finger_client(args.account))
    asyncio.run(tcp_cassandra_client())
