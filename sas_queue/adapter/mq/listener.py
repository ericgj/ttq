import logging
from threading import Thread
from typing import Type, Iterable, Callable

from pika.channel import Channel
from pika import BlockingConnection, ConnectionParameters
from pika.spec import Basic, BasicProperties

logger = logging.getLogger(__name__)

from ...model.mq import MessageContext
from ...model.event import EventProtocol
from ...model.exceptions import (
    EventNotHandled,
)


class Listener(Thread):
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
