import logging
from threading import Event
from time import sleep
from typing import Optional

from pika.blocking_connection import BlockingConnection

from .model.config import Config
from .model.adapter.listener import Listener
from .model.adapter.executor import Executor

logger = logging.getLogger(__name__)


def run(config: Config, stop: Optional[Event] = None):

    sub = BlockingConnection(config.connection)
    pub = BlockingConnection(config.connection)

    executor = Executor(channel=pub.channel(), queue_name=config.publish)
    listener = Listener(
        channel=sub.channel(),
        queue_name=config.subscribe,
        events=config.events,
        executor=executor,
    )

    try:
        listener.start()
        while not stop.is_set():
            sleep(1)

    except KeyboardInterrupt:
        stop.set()

    except Exception as e:
        logger.exception(e)
        raise e

    finally:
        listener.stop()
        executor.shutdown()
        sub.close()
        pub.close()
