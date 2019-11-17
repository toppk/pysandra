import os
import sys
import logging
import typing

_LOGGER_INITIALIZED = False


def get_logger(name: str) -> logging.Logger:
    """
    Get a `logging.Logger` instance, and optionally
    set up debug logging based on the HTTPX_LOG_LEVEL environment variable.
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
