
from .constants import Options, CQL_VERSION, Opcode, Consitency, Flags, SERVER_SENT
from .protocol import Protocol, NETWORK_ORDER, Types, StartupRequest, QueryRequest, get_struct
from .exceptions import VersionMismatchException, InternalDriverError
from .utils import get_logger

logger = get_logger(__name__)

class V4Protocol(Protocol):
    version = 0x04
    def __init__(self, default_flags = 0x00):
        self._default_flags = default_flags

    @property
    def options(self):
        return {Options.CQL_VERSION : CQL_VERSION}

    def flags(self, flags=None):
        if flags is None:
            flags = self._default_flags
        return flags

    def startup(self, stream_id=None, params=None):
        return StartupRequest(version=self.version, flags=self.flags(), options=self.options).to_bytes(stream_id=stream_id)

    def startup_response(self, version, flags, stream, opcode, length, body):
        logger.debug(f"in startup_reponse opcode={opcode}")
        return opcode == Opcode.READY

    def query(self, stream_id=None, params=None):
        return QueryRequest(version=self.version, flags=self.flags(), query=params['query']).to_bytes(stream_id=stream_id)

    def query_response(self, version, flags, stream, opcode, length, body):
        logger.debug(f"body={body}")
        return [body.decode('utf-8')]

    def decode_header(self, header):
        version, flags, stream, opcode, length = get_struct(f"{NETWORK_ORDER}{Types.BYTE}{Types.BYTE}{Types.SHORT}{Types.BYTE}{Types.INT}").unpack(header)
        logger.debug(f"got head={header} containing version={version:x} flags={flags:x} stream={stream:x} opcode={opcode:x} length={length:x}")
        expected_version = SERVER_SENT | self.version
        if version != expected_version:
            raise VersionMismatchException(f"received version={version:x} instead of expected_version={expected_version}")
        return version, flags, stream, opcode, length
        
    
    def decode_body(self, body):
        logger.debug(f"body={body}")
        return body.decode('utf-8')
      
