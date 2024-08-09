from dataclasses import dataclass, asdict
from subprocess import CompletedProcess
from typing import Dict, List, Optional, Type, Callable, Mapping, TypeVar, Any

from ..model.event import EventProtocol
from ..model.message import Context
from ..util.dict_ import excluding_fields


def noop(c: Context) -> None:
    pass


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
    pre_exec: Callable[[Context], None] = noop
    post_exec: Callable[[Context], None] = noop

    def is_success(self, proc: CompletedProcess[str | bytes]) -> bool:
        return self.success_rc == proc.returncode

    def is_warning(self, proc: CompletedProcess[str | bytes]) -> bool:
        return self.warning_rc is not None and self.warning_rc == proc.returncode

    def is_error(self, proc: CompletedProcess[str | bytes]) -> bool:
        return not (self.is_success(proc) or self.is_warning(proc))

    def to_dict(self) -> Dict[str, Any]:
        return excluding_fields(["pre_exec", "post_exec"], asdict(self))


E = TypeVar("E", bound="EventProtocol")
FromEvent = Callable[[E], Command]
EventMapping = Mapping[Type[E], FromEvent[E]]
