from typing import TypeVar, Type, Protocol, Optional

from ..model.command import Command

Self = TypeVar("Self", bound="EventProtocol")


class EventProtocol(Protocol):
    type_name: str = ""
    content_type: str = ""

    @classmethod
    def decode(cls: Type[Self], data: bytes, *, encoding: Optional[str] = None) -> Self:
        ...

    def encode(self: Self, *, encoding: Optional[str] = None) -> bytes:
        ...

    def to_command(self: Self) -> Command:
        ...
