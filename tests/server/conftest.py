import socket
import threading

import pytest

from pysandra.v4protocol import V4Protocol


class Cassim:
    def __init__(self, condition, port=0):
        self.port = port
        self.condition = condition
        self._proto = V4Protocol(server_role=True)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.echo = {
            b"\x04\x00\x00\x00\x05\x00\x00\x00\x00": b"\x84\x00\x00\x00\x06\x00\x00\x00`"
            + b"\x00\x03\x00\x11PROTOCOL_VERSIONS\x00\x03\x00\x043/v3\x00\x044/v4\x00\t5/v5-"
            + b"beta\x00\x0bCOMPRESSION\x00\x02\x00\x06snappy\x00\x03lz4\x00\x0bCQL_VERSION"
            + b"\x00\x01\x00\x053.4.4",  # OPTIONS -> SUPPORTED
            b"\x04\x00\x00\x01\x01\x00\x00\x00\x16"
            + b"\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0": b"\x84\x00\x00\x01\x02\x00\x00\x00\x00",  # STARTUP -> READY
        }

    def __enter__(self):
        self._sock.bind(("127.0.0.1", self.port))
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._sock.close()

    @property
    def addr(self):
        return self._sock.getsockname()

    def listen_for_traffic(self):
        # self.condition.()
        print("\nCASSIM: startup")
        self._sock.listen(5)
        print("CASSIM: listen")
        self.condition.release()
        connection, address = self._sock.accept()
        connected = True
        print("CASSIM: accepted")
        while connected:
            try:
                header = connection.recv(9)
            except OSError as e:
                print(f"CASSIM: got oserror={e}")
                connected = False
                continue
            if len(header) == 0:
                connected = False
                continue
            version, flags, stream, opcode, length = self._proto.decode_header(header)
            body = b""
            if length > 0:
                body = connection.recv(length)
            request = header + body
            if request in self.echo:
                print(f"CASSIM: found header={header!r}")
                connection.send(self.echo[request])
            else:
                print(f"CASSIM: didn't find header={header!r}")
                connected = False
        # connection.close()


@pytest.fixture
def server():
    condition = threading.Semaphore()
    condition.acquire()
    tcp_server = Cassim(condition)
    with tcp_server as example_server:
        thread = threading.Thread(target=example_server.listen_for_traffic)
        thread.daemon = True
        thread.start()
        condition.acquire()
        yield example_server


if __name__ == "__main__":
    import sys

    print(sys.argv)
    port = 0
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    condition = threading.Semaphore()
    condition.acquire()
    tcp_server = Cassim(condition, port=port)
    with tcp_server as example_server:
        thread = threading.Thread(target=example_server.listen_for_traffic)
        thread.start()
        condition.acquire()
        print(example_server.addr)
        # thread.daemon = True
