from concurrent.futures import Future
import logging
from typing import Iterable, Type

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingChannel
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from ..adapter.executor import Executor
from ..model.event import EventProtocol
from ..model.message import Context
from ..model.exceptions import EventNotHandled


class Listener:
    def __init__(
        self,
        *,
        channel: BlockingChannel,
        queue_name: str,
        prefetch_count: int,
        events: Iterable[Type[EventProtocol]],
        executor: Executor,
    ):
        self.channel = channel
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.events = events
        self.executor = executor

        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self._handle
        )

    def _handle(self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes):
        def _handle_result(f: Future):
            logger.debug(
                f"Handling result of command {command.name} for correlation_id {context.correlation_id}"
            )
            try:
                f.result()
                logger.debug(
                    f"Success executing command {command.name} for correlation_id {context.correlation_id}"
                )
                ch.basic_ack(delivery_tag=m.delivery_tag)
            except Exception as e:
                logger.warning(
                    f"Error executing command {command.name} for correlation_id {context.correlation_id}"
                )
                logger.exception(e)
                ch.basic_nack(delivery_tag=m.delivery_tag, requeue=False)

        logger.debug(f"Handling message correlation_id = {p.correlation_id}")
        context = self._context(p, body)
        event = self._decode(body, channel=ch, context=context)
        command = event.to_command()
        logger.debug(f"Submitting command {command.name}")
        f = self.executor.submit(command, context)  # run command in thread + subprocess
        f.add_done_callback(_handle_result)

    def _context(self, properties: BasicProperties, body: bytes) -> Context:
        return Context(
            queue=self.queue_name,
            content_length=len(body),
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            type=properties.type,
            priority=properties.priority,
            correlation_id=properties.correlation_id,
            reply_to=properties.reply_to,
            message_id=properties.message_id,
            timestamp=properties.timestamp,
            user_id=properties.user_id,
            app_id=properties.app_id,
        )

    def _decode(self, body: bytes, channel: Channel, context: Context) -> EventProtocol:
        try:
            return next(
                e.decode(body, encoding=context.content_encoding)
                for e in self.events
                if e.type_name == context.type
                and e.content_type == context.content_type
            )
        except StopIteration:
            raise EventNotHandled(context.type, context.content_type)
