import logging
from typing import Optional, Iterable

from pika import BlockingConnection, ConnectionParameters
from pika.spec import BasicProperties

logger = logging.getLogger(__name__)

from ...model.mq import Queue
from ...model.event import EventProtocol
from ...model.exceptions import (
    InvalidQueuePublish,
    InvalidQueueEventPublish,
    InvalidQueueEventContentPublish,
)


class Publisher:
    def __init__(
        self,
        connection: ConnectionParameters,
        queues: Iterable[Queue],
    ):
        self.queues = queues
        self._channel = BlockingConnection(connection).channel()

    def publish(
        self,
        event: EventProtocol,
        *,
        routing_key: str,
        correlation_id: Optional[str],
        **kwargs,
    ):
        self.validate_event_queue_publish(event, routing_key)

        return self._channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            properties=BasicProperties(correlation_id=correlation_id, **kwargs),
            body=event.encode(),
        )

    def validate_event_queue_publish(self, event: EventProtocol, routing_key: str):
        try:
            event_queue_matches, content_type_accepted, queue_content_types = next(
                (
                    q.name == event.queue,
                    q.accepts(event.content_type),
                    q.accept,
                )
                for q in self.queues
                if q.name == routing_key
            )
        except StopIteration:
            raise InvalidQueuePublish(event_name=event.name, queue_name=routing_key)

        if not event_queue_matches:
            raise InvalidQueueEventPublish(
                event_name=event.name,
                event_queue_name=event.queue,
                queue_name=routing_key,
            )

        if not content_type_accepted:
            raise InvalidQueueEventContentPublish(
                event_name=event.name,
                event_content_type=event.content_type,
                queue_name=routing_key,
                queue_content_types=queue_content_types,
            )
