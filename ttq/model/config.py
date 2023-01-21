from dataclasses import dataclass, asdict
from typing import Any, Type, Optional, List, Dict

from pika import ConnectionParameters

from ..model.event import EventProtocol


@dataclass
class Config:
    connection: ConnectionParameters
    events: List[Type[EventProtocol]]
    storage_file: str
    subscribe: str
    publish: str = ""
    prefetch_count: Optional[int] = None
    max_workers: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
