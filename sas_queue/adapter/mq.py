from concurrent.futures import ThreadPoolExecutor, Future
import logging
from threading import Thread
from typing import Type, Optional, Iterable, Callable

from pika.channel import Channel
from pika import BlockingConnection, ConnectionParameters
from pika.spec import Basic, BasicProperties

logger = logging.getLogger(__name__)

from ..model.mq import MessageContext, Queue
from ..model.event import EventProtocol
from ..model.exceptions import (
    EventNotHandled,
    InvalidQueuePublish,
    InvalidQueueEventPublish,
    InvalidQueueEventContentPublish,
)


class EventListener(Thread):
    def __init__(
        self,
        *,
        connection: ConnectionParameters,
        queue: str,
        max: int = 1,
        events: Iterable[Type[EventProtocol]],
        handle: Callable[[MessageContext, EventProtocol], None],
    ):
        Thread.__init__(self)
        self.connection = connection
        self.queue = queue
        self.max = max
        self.events = events
        self.handle = handle

    def run(self):
        try:
            channel = self.connect_and_open_channel()
            channel.basic_consume(queue=self.queue, on_message_callback=self._handle)
        except Exception as e:
            logger.exception(e)
            raise e  # fail if failed to connect/configure channel

        try:
            channel.start_consuming()
        except Exception as e:
            logger.exception(e)
            self.run()  # keep listening after exception in handling events

    def connect_and_open_channel(self):
        connection = BlockingConnection(self.connection)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        channel.basic_qos(prefetch_count=self.max)
        return channel

    def context(
        self,
        channel: Channel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> MessageContext:
        return MessageContext(
            queue=self.queue,
            content_length=len(body),
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            priority=properties.priority,
            correlation_id=properties.correlation_id,
            reply_to=properties.reply_to,
            message_id=properties.message_id,
            timestamp=properties.timestamp,
            user_id=properties.user_id,
            app_id=properties.app_id,
        )

    def _handle(self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes):
        context = self.context(channel=ch, method=m, properties=p, body=body)
        ctx = context.to_dict()
        ack = True
        try:
            logger.debug("Decoding event", ctx)
            event = self._decode(context, body)
            logger.info("Decoded event", ctx)

            logger.debug("Handling event", ctx)
            self.handle(context, event)
            logger.info("Handled event", ctx)

        except EventNotHandled as e:
            logger.warning(e, ctx)
            ack = False

        except Exception as e:
            logger.warning("Error while handling message", ctx)
            raise e

        finally:
            if m.delivery_tag is None:
                return
            if ack:
                logger.debug("ACK", ctx)
                ch.basic_ack(delivery_tag=m.delivery_tag)
            else:
                logger.debug("NACK", ctx)
                ch.basic_nack(delivery_tag=m.delivery_tag)

    def _decode(self, context, body) -> EventProtocol:
        try:
            return next(
                e.decode(body, encoding=context.content_encoding)
                for e in self.events
                if e.queue == context.queue and e.content_type == context.content_type
            )
        except StopIteration:
            raise EventNotHandled(context.content_type)


class EventPublisher:
    def __init__(
        self,
        connection: ConnectionParameters,
        queues: Iterable[Queue],
        max_workers: Optional[int] = None,
    ):
        self.connection = connection
        self.queues = queues
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="EventPublisher",
        )
        self._connect_and_open_channel()  # TODO is it ok to leave channel open?

    @property
    def max_workers(self) -> int:
        return self._executor._max_workers

    def publish(
        self,
        event: EventProtocol,
        *,
        routing_key: str,
        correlation_id: Optional[str],
        **kwargs,
    ):
        self.validate_event_queue_publish(event, routing_key)

        self._executor.submit(
            self._channel.basic_publish,
            exchange="",
            routing_key=routing_key,
            properties=BasicProperties(correlation_id=correlation_id, **kwargs),
            body=event.encode(),
        ).add_done_callback(
            PublishedEventCallback(
                event, routing_key=routing_key, correlation_id=correlation_id, **kwargs
            )
        )

    def shutdown(self):
        self.logger.debug("Shutting down executor")
        self._executor.shutdown(wait=True)
        self.logger.info("Shut down executor.")

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

    def _connect_and_open_channel(self):
        connection = BlockingConnection(self.connection)
        self._channel = connection.channel()


class PublishedEventCallback:
    def __init__(
        self,
        event: EventProtocol,
        *,
        routing_key: str,
        correlation_id: Optional[str],
        **kwargs,
    ):
        self.event = event
        self.routing_key = routing_key
        self.correlation_id = correlation_id
        self.other_args = kwargs

    def __call__(self, f: Future):
        ctx = {
            "event": self.event.name,
            "routing_key": self.routing_key,
            "correlation_id": self.correlation_id,
            **self.other_args,
        }
        try:
            f.result()
        except Exception as e:
            logger.warning(
                f"Error publishing {self.event.name} with routing key {self.routing_key}",
                ctx,
            )
            logger.exception(e, ctx)
