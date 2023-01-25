from concurrent.futures import Future
import logging
from typing import Iterable, Type

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingChannel
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from ..adapter.executor import Executor
from ..adapter.store import Store
from ..model.event import EventProtocol
from ..model.message import Context
from ..model.exceptions import EventNotHandled, ProcessNotFoundWarning
from ..model.command import Command, FromEvent


class Listener:
    def __init__(
        self,
        *,
        queue_name: str,
        abort_exchange_name: str,
        events: Iterable[Type[EventProtocol]],
        to_command: FromEvent,
        store: Store,
        executor: Executor,
    ):
        self.queue_name = queue_name
        self.abort_exchange_name = abort_exchange_name
        self.events = events
        self.to_command = to_command
        self.store = store
        self.executor = executor

    def abort_queue_name(self, consumer_tag: str) -> str:
        return "-".join([self.queue_name, "abort", consumer_tag])

    def bind(self, channel: BlockingChannel):
        logger.debug(f"Define message handling on queue {self.queue_name}")
        consumer_tag = channel.basic_consume(
            queue=self.queue_name, on_message_callback=self._handle
        )

        abort_queue_name = self.abort_queue_name(consumer_tag)
        logger.debug(f"Declare abort queue {abort_queue_name}")
        channel.queue_declare(abort_queue_name, exclusive=True)

        logger.debug(
            f"Bind abort queue {abort_queue_name} to exchange "
            f"{self.abort_exchange_name} with routing key {consumer_tag}"
        )
        channel.queue_bind(
            queue=abort_queue_name,
            exchange=self.abort_exchange_name,
            routing_key=consumer_tag,
        )

        logger.debug(f"Define abort message handling on queue {abort_queue_name}")
        channel.basic_consume(
            queue=abort_queue_name, on_message_callback=self._handle_abort
        )

    def _handle(self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes):
        context = get_context(self.queue_name, p, body)
        ctx = context.to_dict()

        try:
            logger.debug(
                f"Handling message correlation_id = {context.correlation_id}", ctx
            )
            event = self._decode(body, channel=ch, context=context)

            logger.debug(f"Converting event {event.type_name} to command", ctx)
            command = self.to_command(event)
            self._submit_command(ch, m.delivery_tag, context, command)

        except Exception as e:
            if isinstance(e, Warning):
                logging.info(
                    "Warning submitting command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logging.warning(e, ctx)
            else:
                logging.info(
                    "Error submitting command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logging.exception(e, ctx)

    def _handle_abort(
        self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        context = get_context(self.abort_queue_name, p, body)
        ctx = context.to_dict()

        try:
            command = self._abort_command(context.correlation_id)
            self._submit_command(ch, m.delivery_tag, context, command)

        except Exception as e:
            if isinstance(e, Warning):
                logging.info(
                    "Warning submitting abort command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logging.warning(e, ctx)
            else:
                logging.info(
                    "Error submitting abort command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logging.exception(e, ctx)

    def _submit_command(
        self, channel: Channel, delivery_tag: str, context: Context, command: Command
    ):
        def _handle_result(f: Future):
            logger.debug(
                f"Handling result of command {command.name} for correlation_id {context.correlation_id}",
                ctx,
            )
            try:
                f.result()
                logger.info(
                    f"Success executing command {command.name} for correlation_id {context.correlation_id}",
                    ctx,
                )
                channel.basic_ack(delivery_tag=delivery_tag)
            except Exception as e:
                logger.info(
                    f"Error executing command {command.name} for correlation_id {context.correlation_id}",
                    ctx,
                )
                logger.exception(e, ctx)
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

        ctx = context.to_dict()
        logger.debug(f"Submitting command {command.name}", ctx)
        f = self.executor.submit(  # run command in thread + subprocess
            command=command, context=context, store=self.store
        )
        f.add_done_callback(_handle_result)

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

    def _abort_command(self, correlation_id: str) -> Command:
        pid = self.store.get(correlation_id)
        if pid is None:
            raise ProcessNotFoundWarning(correlation_id)
        return Command(
            name="abort",
            args=["taskkill", "/t", "/pid", str(pid)],
            shell=True,
        )


def get_context(queue_name: str, properties: BasicProperties, body: bytes) -> Context:
    return Context(
        queue=queue_name,
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
