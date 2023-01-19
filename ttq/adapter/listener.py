from typing import Iterable, Type

from pika.blocking_connection import BlockingChannel
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
        events: Iterable[Type[EventProtocol]],
        executor: Executor,
    ):
        self.channel = channel
        self.queue_name = queue_name
        self.events = events
        self.executor = executor

    def run(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self._handle
        )
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()

    def _handle(self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes):
        context = self._context(p, body)
        event = self._decode(body, channel=ch, context=context)
        command = event.to_command()
        self.executor.submit(command, context)  # run command in thread + subprocess

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
