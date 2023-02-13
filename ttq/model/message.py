from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, replace
from typing import Any, Optional, Dict

from pika.spec import Basic, BasicProperties

from ..model.exceptions import (
    MessageMissingRequiredProperty,
    RedeliverLimit,
    RedeliverOriginalExchangeMissing,
)
from ..util import dict_


@dataclass
class Context:
    exchange: str
    routing_key: str
    content: bytes
    correlation_id: str
    reply_to: str
    delivery_mode: int
    content_type: Optional[str]
    content_encoding: Optional[str]
    headers: Optional[Dict[str, str]]
    priority: Optional[int]
    expiration: Optional[str]
    message_id: Optional[str]
    timestamp: Optional[int]
    type: Optional[str]
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
            content=body,
            correlation_id=(  # use message_id if correlation_id not specified
                properties.message_id
                if properties.correlation_id is None
                else properties.correlation_id
            ),
            reply_to=properties.reply_to,
            delivery_mode=properties.delivery_mode,
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            headers=properties.headers,
            priority=properties.priority,
            expiration=properties.expiration,
            message_id=properties.message_id,
            timestamp=properties.timestamp,
            type=properties.type,
            user_id=properties.user_id,
            app_id=properties.app_id,
        )

    @property
    def content_length(self) -> int:
        return len(self.content)

    @property
    def properties(self) -> BasicProperties:
        return BasicProperties(
            reply_to=self.reply_to,
            delivery_mode=self.delivery_mode,
            correlation_id=self.correlation_id,
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            headers=self.headers,
            priority=self.priority,
            expiration=self.expiration,
            message_id=self.message_id,
            timestamp=self.timestamp,
            type=self.type,
            user_id=self.user_id,
            app_id=self.app_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        return dict_.removing({"content"}, asdict(self))


class Redeliver(ABC):
    @property
    @abstractmethod
    def exchange_name(self) -> str:
        ...

    @property
    @abstractmethod
    def routing_key(self) -> str:
        ...

    @abstractmethod
    def delay_context(
        self, context: Context, exchange: str, routing_key: str = ""
    ) -> Context:
        ...

    @abstractmethod
    def return_context(self, context: Context) -> Context:
        ...

    @abstractmethod
    def redelivered_count(self, context: Context) -> int:
        ...

    @abstractmethod
    def delay(self, context: Context) -> float:
        ...


class RedeliverExpDelay(Redeliver):
    """Redeliver with exponential delay"""

    def __init__(
        self,
        *,
        exchange_name: str,
        routing_key: str = "",
        original_exchange_header: str = "x-original-exchange",
        original_routing_key_header: str = "x-original-routing-key",
        redelivered_count_header: str = "x-redelivered-count",
        redeliver_limit: Optional[int] = None,
        initial_delay: Optional[float] = 0,
    ):
        self._exchange_name = exchange_name
        self._routing_key = routing_key
        self.original_exchange_header = original_exchange_header
        self.original_routing_key_header = original_routing_key_header
        self.redelivered_count_header = redelivered_count_header
        self.redeliver_limit = redeliver_limit
        self.initial_delay = initial_delay

    @property
    def exchange_name(self) -> str:
        return self._exchange_name

    @property
    def routing_key(self) -> str:
        return self._routing_key

    def delay_context(self, context: Context) -> Context:
        redelivered_count = self.redelivered_count(context)
        if (
            self.redeliver_limit is not None
            and redelivered_count >= self.redeliver_limit
        ):
            raise RedeliverLimit(
                context.correlation_id,
                header=self.redelivered_count_header,
                count=redelivered_count,
                limit=self.redeliver_limit,
            )

        headers: Dict[str, str] = {} if context.headers is None else context.headers
        return replace(
            context,
            exchange=self._exchange_name,
            routing_key=self._routing_key,
            headers=dict_.merged(
                headers,
                {
                    self.original_exchange_header: context.exchange,
                    self.original_routing_key_header: context.routing_key,
                },
            ),
        )

    def return_context(self, context: Context) -> Context:
        redelivered_count = self.redelivered_count(context)
        original_exchange = self.original_exchange(context)
        original_routing_key = self.original_routing_key(context)
        if original_exchange is None:
            raise RedeliverOriginalExchangeMissing(
                context.correlation_id, header=self.original_exchange_header
            )

        headers: Dict[str, str] = {} if context.headers is None else context.headers
        return replace(
            context,
            exchange=original_exchange,
            routing_key=original_routing_key,
            headers=dict_.removing(
                {self.original_exchange_header, self.original_routing_key_header},
                dict_.merged(
                    headers, {self.redelivered_count_header: redelivered_count + 1}
                ),
            ),
        )

    def delay(self, context: Context) -> float:
        redelivered_count = self.redelivered_count(context)
        return self.initial_delay + (redelivered_count**2.0)

    def redelivered_count(self, context: Context) -> int:
        if context.headers is None:
            return 0
        raw = context.headers.get(self.redelivered_count_header, "0")
        try:
            return int(raw)
        except ValueError:
            return 0

    def original_exchange(self, context: Context) -> Optional[str]:
        if context.headers is None:
            return None
        return context.headers.get(self.original_exchange_header, None)

    def original_routing_key(self, context: Context) -> str:
        if context.headers is None:
            return ""
        return context.headers.get(self.original_routing_key_header, "")
