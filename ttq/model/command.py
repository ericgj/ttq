from dataclasses import dataclass
from typing import List, Optional, Type, Callable, Mapping, TypeVar

from ..model.event import EventProtocol


@dataclass
class Command:
    name: str
    args: List[str]
    shell: bool = False
    cwd: Optional[str] = None
    encoding: Optional[str] = None
    timeout: Optional[float] = None
    success_rc: int = 0
    warning_rc: Optional[int] = None

    def is_success(self, rc: int) -> bool:
        return self.success_rc == rc

    def is_warning(self, rc: int) -> bool:
        return self.warning_rc is not None and self.warning_rc == rc

    def is_error(self, rc: int) -> bool:
        return not (self.is_success(rc) or self.is_warning(rc))


E = TypeVar("E", bound="EventProtocol")
FromEvent = Callable[[E], Command]
EventMapping = Mapping[Type[E], FromEvent]
