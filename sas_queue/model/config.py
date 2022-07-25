from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

from pika import ConnectionParameters


@dataclass
class QueuesConfig:
    subscribe: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Config:
    server: ConnectionParameters
    queues: QueuesConfig
    max_workers: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
