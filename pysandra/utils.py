import logging
import os
import sys
from struct import Struct
from typing import List

from .exceptions import InternalDriverError

_LOGGER_INITIALIZED = False

HAS_SNAPPY = True
try:
    import snappy
except ImportError:
    HAS_SNAPPY = False

HAS_LZ4 = True
try:
    import lz4.block
except ImportError:
    HAS_LZ4 = False


class PKZip:
    def __init__(self) -> None:
        packer = Struct("!l").pack
        supported = {}
        if HAS_SNAPPY:
            supported["snappy"] = (snappy.compress, snappy.uncompress)
        if HAS_LZ4:
            # Cassandra writes the uncompressed message length in big endian order,
            # but the lz4 lib requires little endian order, so we wrap these
            # functions to handle that
            supported["lz4"] = (
                lambda x: packer(len(x)) + lz4.block.compress(x)[4:],
                lambda x: lz4.block.decompress(x[3::-1] + x[4:]),
            )
        self._supported = supported

    @property
    def supported(self) -> List[str]:
        return list(self._supported.keys())

    def compress(self, data: bytes, algo: str) -> bytes:
        if algo not in self._supported:
            raise InternalDriverError(f"not supported algo={algo}")
        return self._supported[algo][0](data)

    def decompress(self, cdata: bytes, algo: str) -> bytes:
        if algo not in self._supported:
            raise InternalDriverError(f"not supported algo={algo}")
        return self._supported[algo][1](cdata)


def get_logger(name: str) -> logging.Logger:
    """
    Get a `logging.Logger` instance, and optionally
    set up debug logging based on the PYSANDRA_LOG_LEVEL environment variable.
    """
    global _LOGGER_INITIALIZED

    if not _LOGGER_INITIALIZED:
        _LOGGER_INITIALIZED = True

        log_level = os.environ.get("PYSANDRA_LOG_LEVEL", "").upper()
        if log_level == "DEBUG":
            logger = logging.getLogger("pysandra")
            logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(
                logging.Formatter(
                    fmt="%(levelname)s [%(asctime)s] %(name)s.%(funcName)s:%(lineno)d - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            logger.addHandler(handler)

    return logging.getLogger(name)
