import json
from typing import Type, Protocol, Optional, TypeVar


C = TypeVar("C", bound="Codec")


class Codec(Protocol):
    """Note: this is the protocol used by JSON Type Definition (jtd)"""

    @classmethod
    def from_json(cls: Type[C], data) -> C:
        ...

    def to_json(self: C):
        ...


class Event:
    type_name = "Event"
    content_type = "application/json"
    codec: Type[Codec]

    def __init__(self, data):
        self._model = self.codec.from_json(data)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"type_name={repr(self.type_name)}, "
            f"content_type={repr(self.content_type)}, "
            f"codec={self.codec}, "
            f"_model={self._model})"
        )

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "Event":
        return cls(
            json.loads(data.decode() if encoding is None else data.decode(encoding))
        )

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        s = json.dumps(self._model.to_json())
        return s.encode() if encoding is None else s.encode(encoding)
