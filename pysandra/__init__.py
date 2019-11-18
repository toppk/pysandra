from .__about__ import __description__, __title__, __version__
from .client import Client
from .utils import get_logger

logger = get_logger(__name__)

__all__ = ["__description__", "__title__", "__version__", "Client"]


logger.warn(
    "***WARNING*** This software is in an early development state.  "
    "There will be bugs and major features missing.  "
    "Please see http://github.com/toppk/pysandra for details!!!"
)
