from typing import Dict, List, Optional

from .constants import CQL_VERSION, DEFAULT_HOST, DEFAULT_PORT, PREFERRED_ALGO, Options
from .exceptions import InternalDriverError
from .utils import PKZip


class Connection:
    def __init__(
        self,
        host: str = None,
        port: int = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        if options is None:
            options = {Options.CQL_VERSION: CQL_VERSION}
        if host is None:
            host = DEFAULT_HOST
        if port is None:
            port = DEFAULT_PORT
        self.host = host
        self.port = port
        self.preferred_algo = PREFERRED_ALGO
        self._options = options
        self._pkzip = PKZip()
        self.supported_options: Optional[Dict[str, List[str]]] = None

    def decompress(self, data: bytes) -> bytes:
        if "COMPRESSION" not in self._options:
            raise InternalDriverError(f"no compression selected")
        return self._pkzip.decompress(data, self._options["COMPRESSION"])

    def make_choices(self, supported_options: Dict[str, List[str]]) -> None:
        self.supported_options = supported_options
        if self.supported_options is not None:
            # check options
            # set compression
            if "COMPRESSION" in self.supported_options:
                matches = [
                    algo
                    for algo in self._pkzip.supported
                    if algo in self.supported_options["COMPRESSION"]
                ]
                if len(matches) > 1:
                    select = (
                        self.preferred_algo
                        if self.preferred_algo in matches
                        else matches[0]
                    )
                    self._options["COMPRESSION"] = select

    @property
    def options(self) -> Dict[str, str]:
        return self._options
