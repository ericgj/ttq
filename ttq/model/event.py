from typing import Protocol, Optional

from ...model.command import Command


class EventProtocol(Protocol):
    content_type: str

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "EventProtocol":
        ...

    @property
    def type_name(self) -> str:
        return self.__class__.__name__

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        ...

    def to_command(self) -> Command:
        ...
