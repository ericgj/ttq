import json
from typing import Type, Protocol, Optional


class Codec(Protocol):
    """Note: this is the protocol used by JSON Type Definition (jtd)"""

    @classmethod
    def from_json(cls, data) -> "Codec":
        ...

    def to_json(self):
        ...


class Event:
    name: str
    queue: str
    content_type: str
    codec: Type[Codec]

    def __init__(self, data):
        self._model = self.codec.from_json(data)

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "Event":
        return cls(
            json.loads(data.decode() if encoding is None else data.decode(encoding))
        )

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        s = json.dumps(self._model.to_json())
        return s.encode() if encoding is None else s.encode(encoding)
