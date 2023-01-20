from concurrent.futures import ThreadPoolExecutor, Future
import logging
from subprocess import Popen, TimeoutExpired, PIPE
from typing import Optional, List

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties

from ..model.command import Command
from ..model.message import Context
from ..model.response import Accepted, Completed

logger = logging.getLogger(__name__)


class Executor:
    def __init__(
        self,
        *,
        channel: BlockingChannel,
        queue_name: str,
        max_workers: Optional[int] = None,
    ):
        self.channel = channel
        self.queue_name = queue_name
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @property
    def max_workers(self) -> int:
        return self._executor._max_workers

    def submit(self, command: Command, context: Context) -> Future:
        f = self._executor.submit(self._exec, command, context)
        return f

    def shutdown(self):
        self._executor.shutdown()

    def _exec(self, command: Command, context: Context):
        with Popen(
            command.args,
            shell=command.shell,
            cwd=command.cwd,
            stdout=PIPE,
            stderr=PIPE,
            encoding=command.encoding,
            text=True,
        ) as p:
            logger.debug(f"Starting process {command.name}, pid = {p.pid}")
            pid = p.pid

            self._store_process_started(pid, context)

            logger.debug(f"Publishing process started to {context.reply_to}")
            self._publish_process_started(context)

            try:
                logger.debug("Running process")
                out, err = p.communicate(timeout=command.timeout)

            except TimeoutExpired:
                logger.warning(
                    f"Process timeout expired at after {command.timeout} secs, killing"
                )
                p.kill()
                logger.debug("Finishing process")
                out, err = p.communicate()

            finally:
                self._store_process_completed(pid, p.returncode, context)

                logger.debug(f"Publishing process completed to {context.reply_to}")
                self._publish_process_completed(
                    args=p.args,
                    returncode=p.returncode,
                    stdout=out,
                    stderr=err,
                    context=context,
                )

    def _publish_process_started(self, context: Context):
        resp = Accepted()
        self.channel.basic_publish(
            exchange="",  # TODO put in config?
            routing_key=context.reply_to,
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=context.content_type,
                correlation_id=context.correlation_id,
            ),
            body=resp.encode(encoding="utf8", content_type=context.content_type),
        )

    def _publish_process_completed(
        self,
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
            stdout=stdout,  # truncate in event.encode if too long?
            stderr=stderr,
        )
        self.channel.basic_publish(
            exchange="",  # TODO put in config?
            routing_key=context.reply_to,
            properties=BasicProperties(
                type=resp.type_name,
                content_encoding="utf8",
                content_type=context.content_type,
                correlation_id=context.correlation_id,
            ),
            body=resp.encode(encoding="utf8", content_type=context.content_type),
        )

    def _store_process_started(self, pid: int, context: Context):
        # TODO
        pass

    def _store_process_completed(
        self, pid: int, returncode: Optional[int], context: Context
    ):
        # TODO
        pass

    def _fetch_pid_by_correlation_id(self, correlation_id: str) -> Optional[int]:
        # TODO
        pass
