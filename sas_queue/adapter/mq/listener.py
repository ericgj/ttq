import logging
from threading import Thread
from typing import Type, Optional, Iterable, Callable

from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.connection import ConnectionParameters
from pika.spec import Basic, BasicProperties
from pika.channel import Channel

logger = logging.getLogger(__name__)

from ...model.mq import MessageContext, Queue
from ...model.event import EventProtocol
from ...model.exceptions import (
    EventNotHandled,
)


class Listener(Thread):
    def __init__(
        self,
        *,
        connection: ConnectionParameters,
        queue: Queue,
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
        self.channel: Optional[BlockingChannel] = None

    def run(self):
        try:
            self.connect_and_open_channel()
            self.channel.basic_consume(
                queue=self.queue.name, on_message_callback=self._handle
            )
        except Exception as e:
            logger.exception(e)
            raise e  # fail if failed to connect/configure channel

        try:
            self.channel.start_consuming()
        except Exception as e:
            logger.exception(e)
            self.run()  # keep listening after exception in handling events

    def stop(self):
        """Called from the main thread"""
        if not self.channel is None:
            self.channel.stop_consuming()
        self.join()

    def connect_and_open_channel(self):
        connection = BlockingConnection(self.connection)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue.name)
        channel.basic_qos(prefetch_count=self.max)
        self.channel = channel

    def context(
        self,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> MessageContext:
        return MessageContext(
            queue=self.queue.name,
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
        context = self.context(method=m, properties=p, body=body)
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
                ch.basic_nack(delivery_tag=m.delivery_tag, requeue=False)

    def _decode(self, context, body) -> EventProtocol:
        try:
            return next(
                e.decode(body, encoding=context.content_encoding)
                for e in self.events
                if e.queue == context.queue and e.content_type == context.content_type
            )
        except StopIteration:
            raise EventNotHandled(context.content_type)
