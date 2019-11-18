import logging
import os
import sys
from typing import Optional, Type, cast

_LOGGER_INITIALIZED = False


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


class SBytes(bytes):
    my_index: Optional[int] = 0

    def __new__(cls: Type[bytes], val: bytes) -> "SBytes":
        return cast(SBytes, super().__new__(cls, val))  # type: ignore # https://github.com/python/typeshed/issues/2630

    def hex(self) -> str:
        return "0x" + super().hex()

    def show(self, count: Optional[int] = None) -> bytes:
        assert self.my_index is not None
        if count is None:
            curindex = self.my_index
            self.my_index = len(self)
            return self[curindex:]

        if self.my_index + count > len(self):
            raise IndexError(f"cannot go beyond {len(self)}")
        curindex = self.my_index
        self.my_index += count
        return self[curindex : curindex + count]

    def at_end(self) -> bool:
        return self.my_index == len(self)


if __name__ == "__main__":
    t = SBytes(b"12345")
    print(f"{t.show(1)!r}{t.at_end()}")
    print(f"{t.show(3)!r}{t.at_end()}")
    print(f"{t.show(2)!r}{t.at_end()}")
