from concurrent.futures import ThreadPoolExecutor
import logging
from subprocess import Popen, TimeoutExpired, PIPE
from typing import Optional, List

from pika.blocking_connection import BlockingChannel
from pika.spec import BasicProperties

from ..model.command import Command
from ..model.message import Context
from ..model.response import Accepted, Completed
from ..util.concurrent.futures import log_exceptions

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

    def submit(self, command: Command, context: Context):
        ctx = context.to_dict()
        f = self._executor.submit(self._exec, command, context)
        f.add_done_callback(
            log_exceptions(
                message=f"Error executing command {command.name} for correlation_id {context.correlation_id}",
                logger=logger,
                context=ctx,
            )
        )

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
            pid = p.pid
            self._store_process_started(pid, context)
            self._publish_process_started(context)
            try:
                out, err = p.communicate(timeout=command.timeout)
            except TimeoutExpired:
                p.kill()
                out, err = p.communicate()
            finally:
                self._store_process_completed(pid, p.returncode, context)
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
