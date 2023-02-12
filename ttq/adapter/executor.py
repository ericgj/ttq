from concurrent.futures import ThreadPoolExecutor, Future
import logging
from subprocess import Popen, TimeoutExpired, PIPE, CompletedProcess
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

from ..adapter.store import Store
from ..adapter.publisher import Publisher
from ..model.command import Command
from ..model.message import Context


class Executor:
    def __init__(
        self,
        *,
        store: Store,
        publisher: Publisher,
        max_workers: Optional[int] = None,
    ):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self.store = store
        self.publisher = publisher

    @property
    def max_workers(self) -> int:
        return self._executor._max_workers

    def submit(self, command: Command, context: Context) -> Future:
        f = self._executor.submit(self._exec, command, context)
        return f

    def shutdown(self):
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _exec(
        self,
        command: Command,
        context: Context,
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
            self.store.put(context.correlation_id, pid)

            logger.debug(
                f"Publishing process {command.name} started to {context.reply_to}", ctx
            )
            self.publisher.publish_started(context)

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
                self.publisher.publish_completed(
                    context=context,
                    proc=CompletedProcess(
                        args=p.args,
                        returncode=p.returncode,
                        stdout=out,
                        stderr=err,
                    ),
                )
                logger.debug(
                    f"Storing process {command.name} completed "
                    f"for correlation_id {context.correlation_id}",
                    ctx,
                )
                self.store.delete(context.correlation_id)

            return (command, p.returncode)
