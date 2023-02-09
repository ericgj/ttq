from concurrent.futures import ThreadPoolExecutor, Future
import logging
from subprocess import Popen, TimeoutExpired, PIPE
from typing import Optional, List, Tuple

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties

from ..adapter.store import Store, Put, Delete
from ..model.command import Command
from ..model.message import Context
from ..model.response import Accepted, Completed

logger = logging.getLogger(__name__)


class Executor:
    def __init__(
        self,
        *,
        channel: BlockingChannel,
        exchange_name: str,
        abort_exchange_name: str,
        max_workers: Optional[int] = None,
    ):
        self.channel = channel
        self.exchange_name = exchange_name
        self.abort_exchange_name = abort_exchange_name
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @property
    def max_workers(self) -> int:
        return self._executor._max_workers

    def submit(self, command: Command, context: Context, store: Store) -> Future:
        f = self._executor.submit(self._exec, command, context, store)
        return f

    def shutdown(self):
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _exec(
        self, command: Command, context: Context, store: Store
    ) -> Tuple[Command, int]:
        with Popen(
            command.args,
            shell=command.shell,
            cwd=command.cwd,
            stdout=PIPE,
            stderr=PIPE,
            encoding=command.encoding,
            text=True,
        ) as p:
            logger.debug(f"Starting process {command.name}")
            pid = p.pid

            logger.debug(
                f"Storing process {command.name} started for correlation_id {context.correlation_id}"
            )
            store.queue.put(Put(context.correlation_id, pid))

            logger.debug(
                f"Publishing process {command.name} started to {context.reply_to}"
            )
            self._publish_process_started(context)

            try:
                logger.debug(f"Running process {command.name}")
                out, err = p.communicate(timeout=command.timeout)

            except TimeoutExpired:
                logger.warning(
                    f"Process {command.name} timeout expired at after {command.timeout} secs, killing"
                )
                p.kill()
                logger.debug(f"Finishing process {command.name}")
                out, err = p.communicate()

            finally:
                logger.debug(
                    f"Publishing process {command.name} completed to {context.reply_to}"
                )
                self._publish_process_completed(
                    args=p.args,
                    returncode=p.returncode,
                    stdout=out,
                    stderr=err,
                    context=context,
                )
                logger.debug(
                    f"Storing process {command.name} completed for correlation_id {context.correlation_id}"
                )
                store.queue.put(Delete(context.correlation_id))

            return (command, p.returncode)

    def _publish_process_started(self, context: Context):
        def _publish():
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=context.reply_to,
                properties=BasicProperties(
                    type=resp.type_name,
                    content_encoding="utf8",
                    content_type=content_type,
                    correlation_id=context.correlation_id,
                ),
                body=resp.encode(encoding="utf8", content_type=content_type),
            )

        resp = Accepted()
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        self.channel.connection.add_callback_threadsafe(_publish)

    def _publish_process_completed(
        self,
        *,
        args: List[str],
        returncode: Optional[int],
        stdout: str,
        stderr: str,
        context: Context,
    ):
        def _publish():
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=context.reply_to,
                properties=BasicProperties(
                    type=resp.type_name,
                    content_encoding="utf8",
                    content_type=content_type,
                    correlation_id=context.correlation_id,
                ),
                body=resp.encode(encoding="utf8", content_type=content_type),
            )

        resp = Completed(
            args=args,
            returncode=returncode,
            stdout=stdout,  # truncate in Completed.encode if too long?
            stderr=stderr,
        )
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        self.channel.connection.add_callback_threadsafe(_publish)
