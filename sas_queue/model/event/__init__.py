from subprocess import CompletedProcess
from typing import Type, Protocol, Optional, List, Tuple

from ...model.command import Command

class EventProtocol(Protocol):
    name: str
    queue: str
    content_type: str

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "EventProtocol":
        ...

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        ...


class EventHandlerProtocol(Protocol):
    name: str
    event_types: List[Type[EventProtocol]]

    def __call__(self, event: EventProtocol) -> Command:
        ...

    def response(self, result: CompletedProcess) -> Tuple[Optional[str],EventProtocol]:
        ...
