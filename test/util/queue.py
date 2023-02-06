from queue import Queue, Empty  # noqa
from typing import TypeVar, Optional, Iterator

A = TypeVar("A")


def queue_iterator(q: "Queue[A]", timeout: Optional[float] = None) -> Iterator[A]:
    while True:
        x = None
        try:
            x = q.get(block=(timeout is not None), timeout=timeout)
        except Empty:
            break

        yield x
        q.task_done()
