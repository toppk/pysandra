import binascii
from struct import pack, unpack, Struct

from .constants import Options, CQL_VERSION, Opcode, Consitency, Flags, SERVER_SENT
from .protocol import Protocol, NETWORK_ORDER, Types
from .exceptions import VersionMismatchException, InternalDriverError

class V4Protocol(Protocol):
    version = 0x04
    def __init__(self, default_flags = 0x00):
        self._default_flags = default_flags
        self._build_structs()

    def _build_structs(self):
        formats = [f"{NETWORK_ORDER}{Types.SHORT}",
                   f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}",
                   f"{NETWORK_ORDER}{Types.SHORT}{Types.BYTE}"]
        structs = {}
            
        for fmt in formats:
            structs[fmt] = Struct(fmt) 
        self._structs = structs

    def _cunpack(self, fmt, data):
        if fmt not in self._structs:
            raise InternalDriverError(f"format={fmt} not cached")
        print(data)
        return self._structs[fmt].unpack(data)

    def _cpack(self, fmt, *args):
        if fmt not in self._structs:
            raise InternalDriverError(f"format={fmt} not cached")
        return self._structs[fmt].pack(*args)

    @property
    def options(self):
        return {Options.CQL_VERSION : CQL_VERSION}

    def flags(self, flags=None):
        if flags is None:
            flags = self._default_flags
        return flags

    def startup(self, stream_id=None):
        #startup_body = pack(f"{NETWORK_ORDER}{Types.SHORT}", len(self.options))
        startup_body = self._cpack(f"{NETWORK_ORDER}{Types.SHORT}", len(self.options))
        for key, value in self.options.items():
            key_bytes = key.encode('utf-8')
            value_bytes = value.encode('utf-8')
            startup_body += pack(f"{NETWORK_ORDER}{Types.String(key_bytes)}{Types.String(value_bytes)}", len(key_bytes), key_bytes, len(value_bytes), value_bytes)
        startup_head = self._cpack(f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}", self.version, self.flags(), stream_id, Opcode.STARTUP, len(startup_body))
        startup_send = startup_head + startup_body
        print("startup_send=%s" % binascii.hexlify(startup_send))
        return startup_send
        
    def query(self, query, stream_id=None):
        query_string = query.encode('utf-8')
        query_body = pack(f"{NETWORK_ORDER}{Types.LongString(query_string)}", len(query_string), query_string)
        #   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>][<keyspace>][<now_in_seconds>]
        query_body += self._cpack(f"{NETWORK_ORDER}{Types.SHORT}{Types.BYTE}", Consitency.ONE, Flags.SKIP_METADATA)
        #query_body += pack("!HL", Consitency.ONE, 0x0002)
        query_head = self._cpack(f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}", self.version, self.flags(), stream_id, Opcode.QUERY, len(query_body))
        query_send = query_head + query_body
        print("query_send=%s" % binascii.hexlify(query_send))
        return query_send


    def decode(self, header):
        print("head=%s" % binascii.hexlify(header))
        version, flags, stream, opcode, length = self._cunpack(f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}", header)
        print("version=%x" % version)
        print("flags=%x" % flags)
        print("stream=%x" % stream)
        print("opcode=%x" % opcode)
        print("length=%x" % length)
        expected_version = SERVER_SENT | self.version
        if version != expected_version:
            raise VersionMismatchException(f"received version={version:x} instead of expected_version={expected_version}")
        return version, flags, stream, opcode, length
        
    
    def decode_body(self, body):
        print("body=%s" % binascii.hexlify(body))
        return body.decode('utf-8')
      
