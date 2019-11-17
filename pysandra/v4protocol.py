
from .constants import Options, CQL_VERSION, Opcode, Consitency, Flags, SERVER_SENT
from .protocol import Protocol, NETWORK_ORDER, Types, StartupRequest, ReadyResponse, QueryRequest, get_struct, ResultResponse
from .exceptions import VersionMismatchException, InternalDriverError, UnknownPayloadException
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
        return StartupRequest(version=self.version, flags=self.flags(), options=self.options, stream_id=stream_id)

    def startup_response(self, version, flags, stream, opcode, length, body):
        logger.debug(f"in startup_reponse opcode={opcode}")
        return opcode == Opcode.READY

    def build_response(self, request, version, flags, stream, opcode, length, body):
        print(request)
        response = None
        if opcode == Opcode.ERROR:
            pass
        elif opcode == Opcode.READY:
            response = ReadyResponse.build(version=version, flags=flags, body=body) 
        elif opcode == Opcode.AUTHENTICATE:
            pass
        elif opcode == Opcode.SUPPORTED:
            pass
        elif opcode == Opcode.RESULT:
            response = ResultResponse.build(version=version, flags=flags, body=body)
        elif opcode == Opcode.EVENT:
            pass
        elif opcode == Opcode.AUTH_CHALLENGE:
            pass
        elif opcode == Opcode.AUTH_SUCCESS:
            pass
        else:
            raise UnknownPayloadException(f"unknown message opcode={opcode}")
        if response is None:
            raise InternalDriverError(f"didn't generate a response message for opcode={opcode}")
        return self.respond(request, response)

    def respond(self, request, response):
        if request.opcode == Opcode.STARTUP:
            if response.opcode == Opcode.READY:
                return True
        elif request.opcode == Opcode.QUERY:
            if response.opcode == Opcode.RESULT:
                logger.debug(f"body={response.body}")
                return [response.body.decode('utf-8')]
        raise InternalDriverError(f"unhandled response={reponse} for request={request}")

    def query(self, stream_id=None, params=None):
        return QueryRequest(version=self.version, flags=self.flags(), query=params['query'], stream_id=stream_id)

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
      
