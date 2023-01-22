from dataclasses import dataclass, asdict
from typing import Any, Optional, Dict

from pika import ConnectionParameters


@dataclass
class Config:
    connection: ConnectionParameters
    storage_file: str
    subscribe: str
    publish: str = ""
    prefetch_count: Optional[int] = None
    max_workers: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
