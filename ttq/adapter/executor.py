from concurrent.futures import ThreadPoolExecutor, Future
import logging
from subprocess import Popen, TimeoutExpired, PIPE
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.connection import ConnectionParameters
from pika.spec import BasicProperties
from pika_pool import QueuedPool

from ..adapter.store import Store, Put, Delete
from ..model.command import Command
from ..model.message import Context
from ..model.response import Accepted, Completed


class Executor:
    # Note: creates a pool of *connections*, not channels off the same
    # connection, because pika connections are not threadsafe.
    # This can be expensive if you have many threads going.

    def __init__(
        self,
        *,
        connection_parameters: ConnectionParameters,
        exchange_name: str,
        abort_exchange_name: str,
        max_workers: Optional[int] = None,
    ):
        self.exchange_name = exchange_name
        self.abort_exchange_name = abort_exchange_name
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._channel_pool = QueuedPool(
            lambda: BlockingConnection(connection_parameters),
            max_size=self._executor._max_workers,
        )

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
        with self._channel_pool.acquire() as conn:
            channel = conn.channel
            if (
                not channel._delivery_confirmation
            ):  # note: pika doesn't officially expose this
                channel.confirm_delivery()
            return self._exec_with_channel(command, context, store, channel)

    def _exec_with_channel(
        self, command: Command, context: Context, store: Store, channel: BlockingChannel
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
            ctx = context.to_dict()
            logger.debug(f"Starting process {command.name}", ctx)
            pid = p.pid

            logger.debug(
                f"Storing process {command.name} started "
                f"for correlation_id {context.correlation_id}",
                ctx,
            )
            store.queue.put(Put(context.correlation_id, pid))

            logger.debug(
                f"Publishing process {command.name} started to {context.reply_to}", ctx
            )
            self._publish_process_started(channel, context)

            logger.info(
                f"Running process {command.name} "
                f"for correlation_id {context.correlation_id}",
                ctx,
            )
            try:
                out, err = p.communicate(timeout=command.timeout)

            except TimeoutExpired:
                logger.warning(
                    f"Process {command.name} timeout expired "
                    f"after {command.timeout} secs, killing",
                    ctx,
                )
                p.kill()
                logger.debug(f"Finishing process {command.name}", ctx)
                out, err = p.communicate()

            finally:
                logger.debug(
                    f"Publishing process {command.name} completed to {context.reply_to}",
                    ctx,
                )
                self._publish_process_completed(
                    channel=channel,
                    args=p.args,
                    returncode=p.returncode,
                    stdout=out,
                    stderr=err,
                    context=context,
                )
                logger.debug(
                    f"Storing process {command.name} completed "
                    f"for correlation_id {context.correlation_id}",
                    ctx,
                )
                store.queue.put(Delete(context.correlation_id))

            return (command, p.returncode)

    def _publish_process_started(self, channel: BlockingChannel, context: Context):
        resp = Accepted()
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        channel.basic_publish(
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

    def _publish_process_completed(
        self,
        channel: BlockingChannel,
        *,
        args: List[str],
        returncode: Optional[int],
        stdout: str,
        stderr: str,
        context: Context,
    ):
        resp = Completed(
            args=args,
            returncode=returncode,
            stdout=stdout,  # truncate in Completed.encode if too long?
            stderr=stderr,
        )
        content_type = (
            "text/plain" if context.content_type is None else context.content_type
        )
        channel.basic_publish(
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
