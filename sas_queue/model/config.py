from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional, Set

from pika import ConnectionParameters

from .model.mq import Queue


@dataclass
class Config:
    server: ConnectionParameters
    subscribe: Queue
    publish: Set[Queue] = field(default_factory=set)
    max_workers: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
