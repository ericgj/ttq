import logging
from queue import Queue, Empty
from threading import Thread, Event
from typing import Type, Union, Any, Optional

logger = logging.getLogger(__name__)

from lmdbm import Lmdb


class Put:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class Delete:
    def __init__(self, key):
        self.key = key


Op = Union[Put, Delete]


class ProcessMap(Lmdb):  # str: int, i.e. correlation_id: pid
    def _pre_key(self, k):
        return k.encode("utf-8")

    def _post_key(self, k):
        return k.decode("utf-8")

    def _pre_value(self, v):
        return v.to_bytes(8, "big", signed=False)

    def _post_value(self, v):
        return int.from_bytes(v, "big", signed=False)


class Store(Thread):
    def __init__(
        self,
        file_name: str,
        lmdb: Type[Lmdb],
        thread_name: Optional[str] = None,
    ):
        self.file_name = file_name
        self.lmdb = lmdb
        self._queue = Queue()
        self._stop_event = Event()
        Thread.__init__(self, name=thread_name)

    @property
    def queue(self) -> Queue:
        return self._queue

    # Q: not a threadsafe read, is this a problem?
    def get(self, key) -> Any:
        with self.lmdb.open(self.file_name, "r") as db:
            return db[key]

    def put(self, key, value):
        self._queue.put(Put(key, value))

    def delete(self, key):
        self._queue.put(Delete(key))

    def run(self):
        logger.debug("Opening lmdb")
        db = self.lmdb.open(self.file_name, "c")

        logger.debug("Listening to queue commands")
        while not self._stop_event.is_set():
            self._handle(db)

    def stop(self):
        self._stop_event.set()
        self.join()
        db = self.lmdb.open(self.file_name, "c")
        self._handle(db)  # one last check of queue from main thread

    def _handle(self, db):
        try:
            op = self._queue.get_nowait()
            if isinstance(op, Put):
                logger.debug("Received: Put")
                db[op.key] = op.value
            elif isinstance(op, Delete):
                logger.debug("Received: Delete")
                del db[op.key]
            else:
                raise ValueError(f"Unknown operation type: {op}")
            self._queue.task_done()
        except Empty:
            pass
        except Exception as e:
            logger.exception(e)
