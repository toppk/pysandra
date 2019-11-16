from .constants import Options, CQL_VERSION
from .protocol import Protocol


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

    
