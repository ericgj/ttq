import logging
from threading import Event
from typing import Optional

from pika.adapters.blocking_connection import BlockingConnection

from ..adapter.listener import Listener
from ..adapter.executor import Executor
from ..adapter.store import Store, ProcessMap
from ..model.config import Config
from ..model import command
from ..util.mapping import compile_type_map

logger = logging.getLogger(__name__)


def run(config: Config, app: command.EventMapping, stop: Optional[Event] = None):

    to_command = compile_type_map(app)
    events = [k for k in app]

    logger.debug("Connecting to subscriber channel")
    sub = BlockingConnection(config.connection)
    sub_ch = sub.channel()
    sub_ch.queue_declare(queue=config.subscribe_queue)
    sub_ch.exchange_declare(exchange=config.subscribe_abort_exchange)

    logger.debug("Connecting to publisher channel")
    pub = BlockingConnection(config.connection)
    pub_ch = pub.channel()

    logger.debug("Starting process map store")
    store = Store(config.storage_file, ProcessMap, thread_name="ProcessMap")
    store.start()

    executor = Executor(
        channel=pub_ch,
        exchange_name=config.publish_exchange,
        abort_exchange_name=config.publish_abort_exchange,
        max_workers=config.max_workers,
    )

    prefetch_count = (
        executor.max_workers if config.prefetch_count is None else config.prefetch_count
    )
    logger.debug(f"Setting prefetch count to {prefetch_count}")
    sub_ch.basic_qos(prefetch_count=prefetch_count)

    listener = Listener(
        queue_name=config.subscribe_queue,
        abort_exchange_name=config.subscribe_abort_exchange,
        events=events,
        to_command=to_command,
        store=store,
        executor=executor,
    )
    logger.debug("Binding subscriber channel consumers")
    listener.bind(sub_ch)

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
