import logging
from threading import Event
from typing import Optional

from pika.adapters.blocking_connection import BlockingConnection

from ..model.config import Config
from ..adapter.listener import Listener
from ..adapter.executor import Executor

logger = logging.getLogger(__name__)


def run(config: Config, stop: Optional[Event] = None):

    logger.debug("Connecting to subscriber channel")
    sub = BlockingConnection(config.connection)
    sub_ch = sub.channel()
    sub_ch.queue_declare(queue=config.subscribe)

    logger.debug("Connecting to publisher channel")
    pub = BlockingConnection(config.connection)
    pub_ch = pub.channel()

    executor = Executor(
        channel=pub_ch,
        queue_name=config.publish,
        max_workers=config.max_workers,
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
        executor=executor,
    )

    try:
        while not stop.is_set():
            sub.sleep(1.0)  # process events in 1-sec intervals

    except KeyboardInterrupt:
        logger.warning("Received CTRL+C, shutting down")
        stop.set()

    except Exception as e:
        logger.exception(e)
        raise e

    finally:
        logger.debug("Shutting down executor")
        executor.shutdown()
        logger.debug("Closing subscriber channel")
        sub.close()
        logger.debug("Closing publisher channel")
        pub.close()
