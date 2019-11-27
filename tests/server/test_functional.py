import socket

import pytest

from pysandra import Client


@pytest.mark.server
def t3st_server_functional_test(server):
    # client = Client(server.con_details())
    # assert client.connect()
    data = ""
    print(server.addr)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(server.addr)
        s.sendall(b"Hello, world")
        data = s.recv(1024)

    assert data.decode() == "Received"


@pytest.mark.server
@pytest.mark.asyncio
async def test_server_functional_connect(server):
    # client = Client(('127.0.0.1', 37891), no_compress=True)
    client = Client(server.addr, no_compress=True)
    # client = Client()
    await client.connect()
    assert client.is_connected
