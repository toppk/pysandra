import logging
import os
import sys

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

    logger = logging.getLogger(name)

    return logger


class SBytes(bytes):
    index = 0

    def __new__(cls, val):
        return super(SBytes, cls).__new__(cls, val)

    def hex(self):
        return "0x" + super().hex()

    def show(self, count=None):
        if count is None:
            curindex = self.index
            self.index = len(self)
            return self[curindex:]

        if self.index + count > len(self):
            raise IndexError(f"cannot go beyond {len(self)}")
        curindex = self.index
        self.index += count
        return self[curindex : curindex + count]

    def at_end(self):
        return self.index == len(self)


if __name__ == "__main__":
    t = SBytes(b"12345")
    print(f"{t.show(1)}{t.at_end()}")
    print(f"{t.show(3)}{t.at_end()}")
    print(f"{t.show(2)}{t.at_end()}")
