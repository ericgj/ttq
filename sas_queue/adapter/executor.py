from concurrent.futures import ThreadPoolExecutor, Future
import logging
from typing import TypeVar, Optional, Iterable, Callable, Tuple

from ..adapter import subprocess_
from ..model.command import Command

logger = logging.getLogger(__name__)

A = TypeVar("A")


class CommandExecutor:
    def __init__(self, max_workers: Optional[int] = None):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="CommandExecutor"
        )

    @property
    def max_workers(self) -> int:
        return self._executor._max_workers

    def submit(
        self,
        cmds: Iterable[Tuple[A, Command]],
        cb: Callable[[A, Command], Callable[[Future], None]],
    ):
        f_map = {
            self._executor.submit(subprocess_.run, cmd): (h, cmd) for (h, cmd) in cmds
        }
        for f in f_map:
            h, cmd = f_map[f]
            f.add_done_callback(cb(h, cmd))

    def shutdown(self):
        self.logger.debug("Shutting down executor")
        self._executor.shutdown(wait=True)
        self.logger.info("Shut down executor.")
