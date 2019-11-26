import importlib
import logging
import os
import sys
from struct import Struct
from types import ModuleType
from typing import Any, Callable, List, Optional


# this voodoo was just to get pytest coverage at 100%
def fetch_module(name: str) -> Optional[ModuleType]:
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        return None


class PKZip:
    def __init__(self) -> None:
        packer = Struct("!l").pack
        supported = {}
        snappy: Any = fetch_module("snappy")
        if snappy is not None:
            assert hasattr(snappy, "compress")
            assert hasattr(snappy, "uncompress")
            supported["snappy"] = (snappy.compress, snappy.uncompress)
        lz4_block: Any = fetch_module("lz4.block")
        if lz4_block is not None:
            assert hasattr(lz4_block, "compress")
            assert hasattr(lz4_block, "decompress")
            # Cassandra writes the uncompressed message length in big endian order,
            # but the lz4 lib requires little endian order, so we wrap these
            # functions to handle that
            supported["lz4"] = (
                lambda x: packer(len(x)) + lz4_block.compress(x)[4:],
                lambda x: lz4_block.decompress(x[3::-1] + x[4:]),
            )
        self._supported = supported

    @property
    def supported(self) -> List[str]:
        return list(self._supported.keys())

    def get_compress(self, algo: str) -> Callable:
        return self._supported[algo][0]

    def get_decompress(self, algo: str) -> Callable:
        return self._supported[algo][1]

    def compress(self, data: bytes, algo: str) -> bytes:
        return self.get_compress(algo)(data)

    def decompress(self, cdata: bytes, algo: str) -> bytes:
        return self.get_decompress(algo)(cdata)


_LOGGER_INITIALIZED = False


def set_debug(enabled: bool) -> None:
    logger = logging.getLogger("pysandra")
    if enabled:
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(levelname)s [%(asctime)s] %(name)s.%(funcName)s:%(lineno)d - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    else:
        logger.setLevel(logging.WARNING)


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
            set_debug(True)

    return logging.getLogger(name)
