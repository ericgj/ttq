import logging
from queue import Queue, Empty
from subprocess import CompletedProcess
from threading import Event, Thread
from typing import Union, Optional

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties

from ..model import command
from ..model.message import Context, Redeliver
from ..model.response import Accepted, Rejected, Started, Completed


class PublishAccepted:
    def __init__(self, context: Context, command: command.Command):
        self.context = context
        self.command = command


class PublishRejected:
    def __init__(self, context: Optional[Context], error: Exception):
        self.context = context
        self.error = error


class PublishStarted:
    def __init__(self, context: Context):
        self.context = context


class PublishCompleted:
    def __init__(self, context: Context, proc: CompletedProcess[str | bytes]):
        self.context = context
        self.proc = proc


class PublishRedeliver:
    def __init__(self, context: Context):
        self.context = context


class PublishStop:
    def __init__(self, exchange_name: str, routing_key: str = ""):
        self.exchange_name = exchange_name
        self.routing_key = routing_key


Command = Union[
    PublishAccepted,
    PublishRejected,
    PublishStarted,
    PublishCompleted,
    PublishRedeliver,
    PublishStop,
]


class Publisher(Thread):
    def __init__(
        self,
        channel: BlockingChannel,
        *,
        exchange_name: str,
        redeliver: Redeliver,
        thread_name: Optional[str] = None,
    ):
        self.channel = channel
        self.exchange_name = exchange_name
        self.redeliver = redeliver
        self._queue: "Queue[Command]" = Queue()
        self._stop_event = Event()
        Thread.__init__(self, name=thread_name)

    @property
    def queue(self) -> "Queue[Command]":
        return self._queue

    def publish_accepted(self, context: Context, command: command.Command) -> None:
        self._queue.put(PublishAccepted(context, command))

    def publish_rejected(self, context: Optional[Context], error: Exception) -> None:
        self._queue.put(PublishRejected(context, error))

    def publish_started(self, context: Context) -> None:
        self._queue.put(PublishStarted(context))

    def publish_completed(
        self, context: Context, proc: CompletedProcess[str | bytes]
    ) -> None:
        self._queue.put(PublishCompleted(context, proc))

    def publish_stop(self, exchange_name: str, routing_key: str = "") -> None:
        self._queue.put(PublishStop(exchange_name, routing_key))

    def publish_redeliver(self, context: Context) -> None:
        self._queue.put(PublishRedeliver(context))

    def run(self) -> None:
        logger.debug("Listening to queue commands")
        while not self._stop_event.is_set():
            self._handle()

    def stop(self) -> None:
        self._stop_event.set()
        self.join()
        self._handle()  # one last check of queue from main thread

    def _handle(self) -> None:
        try:
            cmd = self._queue.get_nowait()
            if isinstance(cmd, PublishAccepted):
                logger.debug("Received: PublishAccepted")
                self._publish_accepted(cmd.context, cmd.command)
            elif isinstance(cmd, PublishRejected):
                logger.debug("Received: PublishRejected")
                if cmd.context is None:
                    self._publish_rejected_no_context(cmd.error)
                else:
                    self._publish_rejected(cmd.context, cmd.error)
            elif isinstance(cmd, PublishStarted):
                logger.debug("Received: PublishStarted")
                self._publish_started(cmd.context)
            elif isinstance(cmd, PublishCompleted):
                logger.debug("Received: PublishCompleted")
                self._publish_completed(cmd.context, cmd.proc)
            elif isinstance(cmd, PublishRedeliver):
                logger.debug("Received: PublishRedeliver")
                self._publish_redeliver(cmd.context)
            elif isinstance(cmd, PublishStop):
                logger.debug("Received: PublishStop")
                self._publish_stop(cmd.exchange_name, cmd.routing_key)
            else:
                raise ValueError(f"Unknown command: {cmd}")

            self._queue.task_done()

        except Empty:
            pass

        except Exception as e:
            # Note errors here (for example message encoding errors, or
            # connection errors) are just swallowed after writing to the log,
            # meaning no response is sent. This is not ideal, but I'm not sure
            # how better to handle it.
            logger.exception(e)

        finally:
            self.channel.connection.process_data_events()

    def _publish_accepted(self, context: Context, command: command.Command) -> None:
        ctx = context.to_dict()
        resp = Accepted(command)
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        logger.debug(
            f"Publishing accepted message for correlation_id = {context.correlation_id}",
            ctx,
        )
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=context.reply_to,
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=content_type,
                correlation_id=context.correlation_id,
                delivery_mode=2,
            ),
            body=resp.encode(encoding="utf8", content_type=content_type),
        )

    def _publish_rejected(self, context: Context, error: Exception) -> None:
        ctx = context.to_dict()
        resp = Rejected(error)
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        logger.debug(
            f"Publishing rejected message for correlation_id = {context.correlation_id}",
            ctx,
        )
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=context.reply_to,
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=content_type,
                correlation_id=context.correlation_id,
                delivery_mode=2,
            ),
            body=resp.encode(encoding="utf8", content_type=content_type),
        )

    def _publish_rejected_no_context(self, error: Exception) -> None:
        resp = Rejected(error)
        content_type = "text/plain"
        logger.debug(
            "Publishing rejected message for unknown correlation_id",
        )
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key="",
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=content_type,
                delivery_mode=2,
            ),
            body=resp.encode(encoding="utf8", content_type=content_type),
        )

    def _publish_started(self, context: Context) -> None:
        ctx = context.to_dict()
        resp = Started()
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        logger.debug(
            f"Publishing started process for correlation_id = {context.correlation_id}",
            ctx,
        )
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=context.reply_to,
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=content_type,
                correlation_id=context.correlation_id,
                delivery_mode=2,
            ),
            body=resp.encode(encoding="utf8", content_type=content_type),
        )

    def _publish_completed(
        self, context: Context, proc: CompletedProcess[str | bytes]
    ) -> None:
        ctx = context.to_dict()
        resp = Completed(
            args=proc.args,
            returncode=proc.returncode,
            stdout=proc.stdout,  # truncate in Completed.encode if too long?
            stderr=proc.stderr,
        )
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        logger.debug(
            f"Publishing completed process for correlation_id = {context.correlation_id}",
            ctx,
        )
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=context.reply_to,
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=content_type,
                correlation_id=context.correlation_id,
                delivery_mode=2,
            ),
            body=resp.encode(encoding="utf8", content_type=content_type),
        )

    def _publish_redeliver(self, context: Context) -> None:
        ctx = context.to_dict()
        redeliver_context = self.redeliver.delay_context(context)
        if self.channel.connection.is_closed:
            logger.warning(
                "Connection already closed, can't redeliver message "
                f"correlation_id = {redeliver_context.correlation_id}",
                ctx,
            )
        else:
            logger.debug(
                "Redelivering message to delay queue "
                f"via exchange {redeliver_context.exchange} "
                f"with routing key '{redeliver_context.routing_key}' "
                f"correlation_id = {redeliver_context.correlation_id}",
                ctx,
            )
            self.channel.basic_publish(
                exchange=redeliver_context.exchange,
                routing_key=redeliver_context.routing_key,
                properties=redeliver_context.properties,
                body=redeliver_context.content,
            )

    def _publish_stop(self, exchange_name: str, routing_key: str) -> None:
        logger.debug("Publishing stop")
        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=b"",
            properties=BasicProperties(
                delivery_mode=2,
            ),
        )
