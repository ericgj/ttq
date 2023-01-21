import logging
from threading import Event
from typing import Optional

from pika.adapters.blocking_connection import BlockingConnection

from ..adapter.listener import Listener
from ..adapter.executor import Executor
from ..adapter.store import Store, ProcessMap
from ..model.config import Config
from ..model import command

logger = logging.getLogger(__name__)


def run(config: Config, app: command.FromEvent, stop: Optional[Event] = None):

    logger.debug("Connecting to subscriber channel")
    sub = BlockingConnection(config.connection)
    sub_ch = sub.channel()
    sub_ch.queue_declare(queue=config.subscribe)

    logger.debug("Connecting to publisher channel")
    pub = BlockingConnection(config.connection)
    pub_ch = pub.channel()

    store = Store(config.storage_file, ProcessMap, thread_name="ProcessMap")
    store.start()

    executor = Executor(
        channel=pub_ch,
        exchange_name=config.publish,
        max_workers=config.max_workers,
        store=store,
    )
    _ = Listener(
        channel=sub_ch,
        queue_name=config.subscribe,
        prefetch_count=(
            executor.max_workers
            if config.prefetch_count is None
            else config.prefetch_count
        ),
        events=config.events,
        app=app,
        executor=executor,
    )

    while not stop.is_set():
        try:
            sub.sleep(1.0)  # process events in 1-sec intervals

        except KeyboardInterrupt:
            logger.warning("Received CTRL+C, shutting down")
            stop.set()
            continue

        except Exception as e:
            logger.exception(e)
            raise e

        finally:
            logger.debug("Shutting down executor")
            executor.shutdown()
            logger.debug("Stopping store")
            store.stop()
            logger.debug("Closing subscriber channel")
            sub.close()
            logger.debug("Closing publisher channel")
            pub.close()
