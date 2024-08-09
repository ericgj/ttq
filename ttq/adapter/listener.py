from concurrent.futures import Future, CancelledError
from functools import partial
import logging
from subprocess import CompletedProcess
from typing import Iterable, Type, Tuple, Optional, Any

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from ..adapter.executor import Executor
from ..adapter.store import Store
from ..adapter.publisher import Publisher
from ..model.event import EventProtocol
from ..model.message import Context
from ..model.exceptions import EventNotHandled, ProcessNotFoundWarning
from ..model.command import Command, FromEvent


def ack(channel: BlockingChannel, delivery_tag: int, context: Context) -> None:
    ctx = context.to_dict()
    logger.debug(f"ACK message correlation_id = {context.correlation_id}", ctx)
    channel.basic_ack(delivery_tag=delivery_tag)


def nack(
    channel: BlockingChannel, delivery_tag: int, properties: BasicProperties
) -> None:
    logger.warning(f"NACK message, properties = {properties}")
    channel.basic_nack(delivery_tag=delivery_tag, requeue=False)


class Listener:
    def __init__(
        self,
        *,
        queue_name: str,
        abort_exchange_name: str,
        events: Iterable[Type[EventProtocol]],
        to_command: FromEvent[EventProtocol],
        store: Store,
        publisher: Publisher,
        executor: Executor,
    ):
        self.queue_name = queue_name
        self.abort_exchange_name = abort_exchange_name
        self.events = events
        self.to_command = to_command
        self.store = store
        self.publisher = publisher
        self.executor = executor

    def bind(self, channel: BlockingChannel) -> Any:  # should be str (consumer tag)
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
        r = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
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
    ) -> None:
        # Note: unbinding and deleting queue not needed, transient queue takes care of itself
        ctx = context.to_dict()
        logger.debug(
            f"Unbinding message handling on transient abort queue {queue_name}", ctx
        )
        channel.basic_cancel(consumer_tag)  # type: ignore[no-untyped-call]

    def _handle(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ) -> None:
        _nack = partial(nack, ch, m.delivery_tag, p)
        context: Optional[Context] = None
        command: Optional[Command] = None
        try:
            logger.debug("Message received, getting context")
            context = Context.from_event(m, p, body)
            ctx = context.to_dict()

            logger.info(
                f"Handling message correlation_id = {context.correlation_id}", ctx
            )

            _ack = partial(ack, ch, m.delivery_tag, context)

            logger.debug(
                f"Decoding event from message correlation_id = {context.correlation_id}",
                ctx,
            )
            event = self._decode(context=context)

            logger.debug(f"Converting event {event.type_name} to command", ctx)
            command = self.to_command(event)

            logger.debug(
                f"Submitting command {command.name} "
                f"for message correlation_id = {context.correlation_id}",
                ctx,
            )
            self._submit_command(channel=ch, context=context, command=command)
            self._accept_message(context=context, command=command)
            ch.connection.add_callback_threadsafe(_ack)

        except Exception as e:
            self._reject_message(error=e, properties=p, context=context)
            ch.connection.add_callback_threadsafe(_nack)

    def _handle_abort(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ) -> None:
        _nack = partial(nack, ch, m.delivery_tag, p)
        context: Optional[Context] = None
        command: Optional[Command] = None
        try:
            logger.debug("Message received, getting context")
            context = Context.from_event(m, p, body)
            ctx = context.to_dict()

            logger.info(
                f"Handling message correlation_id = {context.correlation_id}", ctx
            )

            _ack = partial(ack, ch, m.delivery_tag, context)

            logger.debug(
                f"Generating abort command for correlation_id = "
                f"{context.routing_key}",
                ctx,
            )
            command = self._abort_command(context.routing_key)

            logger.debug(
                f"Submitting command {command.name} "
                f"for message correlation_id = {context.correlation_id}",
                ctx,
            )
            self._submit_command(channel=ch, context=context, command=command)
            self._accept_message(context=context, command=command)
            ch.connection.add_callback_threadsafe(_ack)

        except Exception as e:
            self._reject_message(error=e, properties=p, context=context)
            ch.connection.add_callback_threadsafe(_nack)

    def _accept_message(
        self,
        context: Context,
        command: Command,
    ) -> None:
        ctx = context.to_dict()
        logger.info(
            f"Submitted command {command.name} for correlation_id = "
            f"{context.correlation_id}",
            ctx,
        )
        self.publisher.publish_accepted(context=context, command=command)

    def _reject_message(
        self,
        error: Exception,
        properties: BasicProperties,
        context: Optional[Context],
    ) -> None:
        if context is None:
            logger.info(
                f"Error getting context from message, properties = " f"{properties}",
            )
            logger.exception(error)
            self.publisher.publish_rejected(error=error, context=context)

        else:
            ctx = context.to_dict()
            logger.info(
                f"Error submitting command for correlation_id = "
                f"{context.correlation_id}",
                ctx,
            )
            logger.exception(error, ctx)
            self.publisher.publish_rejected(error=error, context=context)

    def _submit_command(
        self, channel: BlockingChannel, context: Context, command: Command
    ) -> None:
        abort_queue_name, abort_consumer_tag = self.bind_abort(channel, context)

        exec_handler = ExecutionHandler(
            context=context,
            command=command,
            publisher=self.publisher,
        )
        unbind_abort = partial(
            self.unbind_abort,
            channel,
            context,
            consumer_tag=abort_consumer_tag,
            queue_name=abort_queue_name,
        )

        f = self.executor.submit(command=command, context=context)
        f.add_done_callback(
            lambda _: channel.connection.add_callback_threadsafe(unbind_abort)
        )
        f.add_done_callback(exec_handler)

    def _decode(self, context: Context) -> EventProtocol:
        try:
            return next(
                e.decode(context.content, encoding=context.content_encoding)
                for e in self.events
                if e.type_name == context.type
                and e.content_type == context.content_type
            )
        except StopIteration:
            raise EventNotHandled(
                "(unspecified)" if context.type is None else context.type,
                "(unspecified)"
                if context.content_type is None
                else context.content_type,
            )

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


class ExecutionHandler:
    """Execution error handling"""

    def __init__(
        self,
        context: Context,
        command: Command,
        publisher: Publisher,
    ):
        self.context = context
        self.command = command
        self.publisher = publisher

    def __call__(
        self, f: Future[Tuple[Command, CompletedProcess[str | bytes]]]
    ) -> None:
        # Note: this is run from the executor threads, and any exception
        # here is swallowed. Hence the cautious error handling.

        command = self.command
        context = self.context
        ctx = context.to_dict()

        try:
            # Note: exception thrown here if any error in executor thread,
            # but note this _does not_ include any error in subprocess.
            # Subprocess errors are handled as normal results. Exceptions
            # might be thrown here running pre- and post-exec routines,
            # publishing results, storing PID, etc.

            cmd: Command
            proc: CompletedProcess[str | bytes]

            cmd, proc = f.result()
            if cmd.is_error(proc):
                logger.error(
                    f"Error ({proc.returncode}) executing command {command.name} "
                    f"for correlation_id {context.correlation_id}",
                    ctx,
                )
            elif cmd.is_warning(proc):
                logger.warning(
                    f"Warning ({proc.returncode}) executing command {command.name} "
                    f"for correlation_id {context.correlation_id}",
                    ctx,
                )
            elif cmd.is_success(proc):
                logger.info(
                    f"Success ({proc.returncode}) executing command {command.name} "
                    f"for correlation_id {context.correlation_id}",
                    ctx,
                )

        except CancelledError:
            logger.warning(
                f"During shutdown, command {command.name} "
                f"for correlation_id {context.correlation_id} was cancelled, "
                "retrying",
                ctx,
            )
            try:
                self.publisher.publish_redeliver(context=context)
            except Exception as e:
                logger.exception(e, ctx)

        except Exception as e:
            logger.info(
                f"Internal error executing command {command.name} "
                f"for correlation_id {context.correlation_id}",
                ctx,
            )
            logger.exception(e, ctx)


class Shutdown:
    """
    Receive shutdown messages, shut down resources gracefully
    """

    def __init__(
        self,
        exchange_name: str,
        routing_key: str,
        executor: Executor,
        store: Store,
        publisher: Publisher,
    ):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.executor = executor
        self.store = store
        self.publisher = publisher

    def bind(self, channel: BlockingChannel) -> None:
        logger.debug("Declare transient shutdown queue")
        r = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        queue_name = r.method.queue

        logger.debug(
            f"Bind transient shutdown queue {queue_name} to exchange {self.exchange_name}"
        )
        channel.queue_bind(
            exchange=self.exchange_name,
            queue=queue_name,
            routing_key=self.routing_key,
        )

        logger.debug(f"Bind message handling on transient shutdown queue {queue_name}")
        channel.basic_consume(
            queue=queue_name, auto_ack=True, on_message_callback=self._handle
        )

    def _handle(
        self,
        channel: BlockingChannel,
        m: Basic.Deliver,
        p: BasicProperties,
        body: bytes,
    ) -> None:
        logger.debug("Shutting down executor, cancelling pending jobs")
        self.executor.shutdown()

        logger.debug("Stopping store")
        self.store.stop()

        logger.debug("Stopping publisher")
        self.publisher.stop()

        channel.stop_consuming()  # !!!
