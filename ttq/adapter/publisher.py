import logging
from queue import Queue, Empty
from subprocess import CompletedProcess
from threading import Event, Thread
from typing import Union, Optional

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties

from ..model.message import Context
from ..model.response import Accepted, Completed


class PublishStarted:
    def __init__(self, context: Context):
        self.context = context


class PublishCompleted:
    def __init__(self, context: Context, proc: CompletedProcess):
        self.context = context
        self.proc = proc


class PublishStop:
    def __init__(self, exchange_name: str, routing_key: str = ""):
        self.exchange_name = exchange_name
        self.routing_key = routing_key


Command = Union[PublishStarted, PublishCompleted, PublishStop]


class Publisher(Thread):
    def __init__(
        self,
        channel: BlockingChannel,
        *,
        exchange_name: str,
        thread_name: Optional[str] = None,
    ):
        self.channel = channel
        self.exchange_name = exchange_name
        self._queue: "Queue[Command]" = Queue()
        self._stop_event = Event()
        Thread.__init__(self, name=thread_name)

    @property
    def queue(self) -> "Queue[Command]":
        return self._queue

    def publish_started(self, context: Context):
        self._queue.put(PublishStarted(context))

    def publish_completed(self, context: Context, proc: CompletedProcess):
        self._queue.put(PublishCompleted(context, proc))

    def publish_stop(self, exchange_name: str, routing_key: str = ""):
        self._queue.put(PublishStop(exchange_name, routing_key))

    def run(self):
        logger.debug("Listening to queue commands")
        while not self._stop_event.is_set():
            self._handle()

    def stop(self):
        self._stop_event.set()
        self.join()
        self._handle()  # one last check of queue from main thread

    def _handle(self):
        try:
            cmd = self._queue.get_nowait()
            if isinstance(cmd, PublishStarted):
                logger.debug("Received: PublishStarted")
                self._publish_started(cmd.context)
            elif isinstance(cmd, PublishCompleted):
                logger.debug("Received: PublishCompleted")
                self._publish_completed(cmd.context, cmd.proc)
            elif isinstance(cmd, PublishStop):
                logger.debug("Received: PublishStop")
                self._publish_stop(cmd.exchange_name, cmd.routing_key)
            else:
                raise ValueError(f"Unknown command: {cmd}")

            self._queue.task_done()

        except Empty:
            pass

        except Exception as e:
            logger.exception(e)

        finally:
            self.channel.connection.process_data_events()

    def _publish_started(self, context: Context):
        resp = Accepted()
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        logger.debug(
            f"Publishing started process for correlation_id = {context.correlation_id}"
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

    def _publish_completed(self, context: Context, proc: CompletedProcess):
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
            f"Publishing completed process for correlation_id = {context.correlation_id}"
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

    def _publish_stop(self, exchange_name: str, routing_key: str):
        logger.debug("Publishing stop")
        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=b"",
            properties=BasicProperties(
                delivery_mode=2,
            ),
        )
