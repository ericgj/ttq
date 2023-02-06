from concurrent.futures import Future
import logging
from typing import Iterable, Type, Tuple

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

    def bind(self, channel: BlockingChannel) -> str:
        logger.debug(f"Bind message handling on queue {self.queue_name}")
        return channel.basic_consume(
            queue=self.queue_name, on_message_callback=self._handle
        )

    def bind_abort(self, channel: BlockingChannel, context: Context) -> Tuple[str, str]:
        ctx = context.to_dict()
        logger.debug(
            f"Declare transient abort queue for correlation_id {context.correlation_id}",
            ctx,
        )
        r = channel.queue_declare(queue="", exclusive=True)
        abort_queue_name = r.method.queue

        logger.debug(
            f"Bind transient abort queue {abort_queue_name} to exchange "
            f"{self.abort_exchange_name} with correlation_id {context.correlation_id} "
            "as the routing key",
            ctx,
        )
        channel.queue_bind(
            exchange=self.abort_exchange_name,
            queue=abort_queue_name,
            routing_key=context.correlation_id,
        )

        logger.debug(
            f"Bind message handling on transient abort queue {abort_queue_name}", ctx
        )
        consumer_tag = channel.basic_consume(
            queue=abort_queue_name, on_message_callback=self._handle_abort
        )
        return (abort_queue_name, consumer_tag)

    def unbind_abort(
        self,
        channel: BlockingChannel,
        context: Context,
        *,
        consumer_tag: str,
        queue_name: str,
    ):
        """
        Note: unbinding and deleting queue not needed, transient queue takes care of itself
        Also note this is run from an executor thread, hence the threadsafe operation
        """

        def _unbind():
            channel.basic_cancel(consumer_tag)

        ctx = context.to_dict()
        logger.debug(
            f"Unbinding message handling on transient abort queue {queue_name}", ctx
        )
        channel.connection.add_callback_threadsafe(_unbind)

    def _handle(self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes):
        def _ack():
            ch.basic_ack(delivery_tag=m.delivery_tag)

        def _nack():
            ch.basic_nack(delivery_tag=m.delivery_tag, requeue=False)

        context = get_context(m, p, body)
        ctx = context.to_dict()

        try:
            logger.debug(
                f"Handling message correlation_id = {context.correlation_id}", ctx
            )
            event = self._decode(body, channel=ch, context=context)

            logger.debug(f"Converting event {event.type_name} to command", ctx)
            command = self.to_command(event)
            self._submit_command(ch, context, command)
            ch.connection.add_callback_threadsafe(_ack)

        except Exception as e:
            if isinstance(e, Warning):
                ch.connection.add_callback_threadsafe(_ack)
                logging.info(
                    "Warning submitting command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logging.warning(e, ctx)
            else:
                ch.connection.add_callback_threadsafe(_nack)
                logging.info(
                    "Error submitting command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logging.exception(e, ctx)

    def _handle_abort(
        self, ch: Channel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        def _ack():
            ch.basic_ack(delivery_tag=m.delivery_tag)

        def _nack():
            ch.basic_nack(delivery_tag=m.delivery_tag, requeue=False)

        context = get_context(m, p, body)
        ctx = context.to_dict()

        try:
            command = self._abort_command(context.routing_key)
            self._submit_command(ch, context, command)
            ch.connection.add_callback_threadsafe(_ack)

        except Exception as e:
            if isinstance(e, Warning):
                ch.connection.add_callback_threadsafe(_ack)
                logger.info(
                    "Warning submitting abort command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logger.warning(e, ctx)
            else:
                ch.connection.add_callback_threadsafe(_nack)
                logger.info(
                    "Error submitting abort command from message correlation_id = "
                    f"{context.correlation_id}",
                    ctx,
                )
                logger.exception(e, ctx)

    def _submit_command(self, channel: Channel, context: Context, command: Command):
        def _handle_result(f: Future):
            # Note: this is run from the executor threads, and any exception
            # here is swallowed. Hence the cautious error handling.

            try:
                # Note: exception thrown here if any error in executor thread,
                # but note this _does not_ include any error in subprocess.
                # Subprocess errors are handled as normal results. Exceptions
                # might be thrown here publishing results, storing PID, etc.

                cmd, rc = f.result()
                if cmd.is_error(rc):
                    logger.error(
                        f"Error ({rc}) executing command {command.name} for correlation_id {context.correlation_id}",
                        ctx,
                    )
                elif cmd.is_warning(rc):
                    logger.warning(
                        f"Warning ({rc}) executing command {command.name} for correlation_id {context.correlation_id}",
                        ctx,
                    )
                elif cmd.is_success(rc):
                    logger.info(
                        f"Success ({rc}) executing command {command.name} for correlation_id {context.correlation_id}",
                        ctx,
                    )

            except Exception as e:
                logger.info(
                    f"Error executing command {command.name} for correlation_id {context.correlation_id}",
                    ctx,
                )
                logger.exception(e, ctx)

            finally:
                # Attempt to unbind and delete the transient abort queue
                # connected to this task (via correlation_id routing key)
                # Note this is done threadsafe

                try:
                    self.unbind_abort(
                        channel,
                        context,
                        queue_name=abort_queue_name,
                        consumer_tag=abort_consumer_tag,
                    )
                except Exception as e:
                    logger.info(
                        f"Error unbinding transient abort queue {abort_queue_name}, ignoring",
                        ctx,
                    )
                    logger.exception(e, ctx)

        ctx = context.to_dict()

        logger.debug(f"Submitting command {command.name}", ctx)
        f = self.executor.submit(  # run command in thread + subprocess
            command=command, context=context, store=self.store
        )
        f.add_done_callback(_handle_result)
        abort_queue_name, abort_consumer_tag = self.bind_abort(channel, context)

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
        pid: int
        try:
            pid = self.store.get(correlation_id)
        except KeyError:
            raise ProcessNotFoundWarning(correlation_id)
        return Command(
            name="abort",
            args=["taskkill", "/t", "/f", "/pid", str(pid)],
            shell=True,
        )


def get_context(
    method: Basic.Deliver, properties: BasicProperties, body: bytes
) -> Context:
    return Context(
        exchange=method.exchange,
        routing_key=method.routing_key,
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
