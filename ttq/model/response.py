from dataclasses import dataclass, asdict
import json
from typing import List, Protocol, Optional, Dict, Any

from ..model.command import Command
from ..model.exceptions import NoEncodingForContentType


class Response(Protocol):
    type_name: str = "Response"

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes: ...


@dataclass
class Accepted:
    type_name = "Accepted"
    command: Command

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        if content_type == "text/plain":
            return b""
        elif content_type == "application/json":
            c = {"command": self.command.to_dict()}
            s = json.dumps(c)
            return s.encode() if encoding is None else s.encode(encoding)
        else:
            raise NoEncodingForContentType(self.type_name, content_type)


@dataclass
class Rejected:
    type_name = "Rejected"
    error: Exception

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        if content_type == "text/plain":
            return (
                str(self.error).encode()
                if encoding is None
                else str(self.error).encode(encoding)
            )
        elif content_type == "application/json":
            e = {
                "error": {
                    "type": self.error.__class__.__name__,
                    "message": str(self.error),
                }
            }
            s = json.dumps(e)
            return s.encode() if encoding is None else s.encode(encoding)
        else:
            raise NoEncodingForContentType(self.type_name, content_type)


class Started:
    type_name = "Started"

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        if content_type == "text/plain":
            return b""
        elif content_type == "application/json":
            s = json.dumps({})
            return s.encode() if encoding is None else s.encode(encoding)
        else:
            raise NoEncodingForContentType(self.type_name, content_type)


@dataclass
class Completed:
    type_name = "Completed"
    args: List[str]
    returncode: int
    stdout: str | bytes
    stderr: str | bytes

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def encode(self, *, content_type: str, encoding: Optional[str] = None) -> bytes:
        if content_type == "text/plain":
            s = str(self.returncode)
            return s.encode() if encoding is None else s.encode(encoding)
        elif content_type == "application/json":
            s = json.dumps(self.to_dict())
            return s.encode() if encoding is None else s.encode(encoding)
        else:
            raise NoEncodingForContentType(self.type_name, content_type)
