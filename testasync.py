#!/usr/bin/python3

from pysandra.constants import *
from pysandra.protocol import *
from pysandra.connection import default_host
from pysandra.v4protocol import V4Protocol
from pysandra.client import Client

import asyncio
from struct import pack, unpack
import binascii

async def tcp_cassandra_client():

    client = Client()

    reader, writer = await asyncio.open_connection(*default_host())
    options = client.protocol.options
    
    startup_body = pack(f"{NETWORK_ORDER}{Types.SHORT}", len(options))
    for key, value in options.items():
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        startup_body += pack(f"{NETWORK_ORDER}{Types.SHORT}{Types.String(key_bytes)}{Types.SHORT}{Types.String(value_bytes)}", len(key_bytes), key_bytes, len(value_bytes), value_bytes)
    startup_head = pack(f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.UINT}", client.protocol.version, client.protocol.flags(), client.newstreamid(), Opcode.STARTUP, len(startup_body))
    startup_send = startup_head + startup_body
    print("send=%s" % binascii.hexlify(startup_send))
    
    writer.write(startup_send)
    #await writer.drain()

    head = await reader.read(9)
    print("head=%s" % binascii.hexlify(head))
    version, flags, stream, opcode, length = unpack('!BBHBL', head)
    print("version=%x" % version)
    print("flags=%x" % flags)
    print("stream=%x" % stream)
    print("opcode=%x" % opcode)
    print("length=%x" % length)
    body = await reader.read(length)
    
    print("body=%s" % binascii.hexlify(body))


    query_string = "SELECT * FROM uprofile.user where user_id=1".encode('utf-8')
    query_body = pack(f"!L{len(query_string)}s", len(query_string), query_string)
    #   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>][<keyspace>][<now_in_seconds>]
    query_body += pack("!HL", Consitency.ONE, 0x0002)
    query_head = pack('!BBHBL', 0x04, 0x00, 0x00, 0x07, len(query_body))
    query_send = query_head + query_body
    print("send=%s" % binascii.hexlify(query_send))
    
    writer.write(query_send)

    head = await reader.read(9)
    print("head=%s" % binascii.hexlify(head))
    version, flags, stream, opcode, length = unpack('!BBHBL', head)
    print("version=%x" % version)
    print("flags=%x" % flags)
    print("stream=%x" % stream)
    print("opcode=%x" % opcode)
    print("length=%x" % length)
    body = await reader.read(length)


    print("body=%s" % binascii.hexlify(body))
    print(body.decode('utf-8'))
    
    writer.close()
    await writer.wait_closed()
    

import argparse
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("account")
    args = parser.parse_args()
    #asyncio.run(tcp_finger_client(args.account))
    asyncio.run(tcp_cassandra_client())
