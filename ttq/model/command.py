from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Command:
    name: str
    args: List[str]
    shell: bool = False
    cwd: Optional[str] = None
    encoding: Optional[str] = None
    timeout: Optional[float] = None
