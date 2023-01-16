from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Dict, Set


@dataclass(eq=True, frozen=True)
class Queue:
    name: str
    accept: Set[str] = field(default_factory=set)

    def accepts(self, content_type: str) -> bool:
        return content_type in self.accept


@dataclass(eq=True, frozen=True)
class MessageContext:
    queue: str
    content_length: int
    content_type: Optional[str]
    content_encoding: Optional[str]
    priority: Optional[int]
    correlation_id: Optional[str]
    reply_to: Optional[str]
    message_id: Optional[str]
    timestamp: Optional[int]
    user_id: Optional[str]
    app_id: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
