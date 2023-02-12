from dataclasses import dataclass, asdict
from typing import Any, Optional, Dict

from pika.spec import Basic, BasicProperties

from ..model.exceptions import MessageMissingRequiredProperty


@dataclass
class Context:
    exchange: str
    routing_key: str
    content_length: int
    correlation_id: str
    reply_to: str
    type: Optional[str]
    content_type: Optional[str]
    content_encoding: Optional[str]
    priority: Optional[int]
    message_id: Optional[str]
    timestamp: Optional[int]
    user_id: Optional[str]
    app_id: Optional[str]

    @classmethod
    def from_event(
        cls, method: Basic.Deliver, properties: BasicProperties, body: bytes
    ):
        if properties.reply_to is None:
            raise MessageMissingRequiredProperty("reply_to", properties)
        if properties.message_id is None and properties.correlation_id is None:
            raise MessageMissingRequiredProperty(
                "message_id or correlation_id", properties
            )

        return cls(
            exchange=method.exchange,
            routing_key=method.routing_key,
            content_length=len(body),
            type=properties.type,
            correlation_id=(  # use message_id if correlation_id not specified
                properties.message_id
                if properties.correlation_id is None
                else properties.correlation_id
            ),
            reply_to=properties.reply_to,
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            priority=properties.priority,
            message_id=properties.message_id,
            timestamp=properties.timestamp,
            user_id=properties.user_id,
            app_id=properties.app_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
