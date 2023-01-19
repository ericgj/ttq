from dataclasses import dataclass, asdict
import json
from typing import Protocol, List, Optional, Dict, Any

from ..model.exceptions import NoEncodingForContentType


class Response(Protocol):
    @property
    def type_name(self) -> str:
        return self.__class__.__name__

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        ...


class Accepted:
    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        if content_type == "text/plain":
            return b""
        if content_type == "application/json":
            s = json.dumps({})
            return s.encode() if encoding is None else s.encode(encoding)
        raise NoEncodingForContentType(self.type_name, content_type)


@dataclass
class Completed:
    args: List[str]
    returncode: int
    stdout: Optional[str]
    stderr: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        if content_type == "text/plain":
            s = str(self.returncode)
            return s.encode() if encoding is None else s.encode(encoding)
        if content_type == "application/json":
            s = json.dumps(self.to_dict())
            return s.encode() if encoding is None else s.encode(encoding)
        raise NoEncodingForContentType(self.type_name, content_type)
