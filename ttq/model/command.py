from dataclasses import dataclass
from typing import List, Optional, Type, Callable, Mapping

from ..model.event import EventProtocol


@dataclass
class Command:
    name: str
    args: List[str]
    shell: bool = False
    cwd: Optional[str] = None
    encoding: Optional[str] = None
    timeout: Optional[float] = None


FromEvent = Callable[[Type[EventProtocol]], Command]
EventMapping = Mapping[Type[EventProtocol], FromEvent]
