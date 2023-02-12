import logging

from pika.adapters.blocking_connection import BlockingConnection

from ..adapter.listener import Listener, Shutdown
from ..adapter.executor import Executor
from ..adapter.store import Store, ProcessMap
from ..adapter.publisher import Publisher
from ..model.config import Config
from ..model import command
from ..util.mapping import compile_type_map

logger = logging.getLogger(__name__)


def run(config: Config, app: command.EventMapping):
    to_command = compile_type_map(app)
    events = [k for k in app]

    logger.debug("Connecting to subscriber channel")
    sub = BlockingConnection(config.connection)
    sub_ch = sub.channel()

    # don't do this here ? Or maybe add a config option ?
    # sub_ch.queue_declare(queue=config.request_queue)
    # sub_ch.exchange_declare(exchange=config.request_abort_exchange)

    logger.debug("Connecting to publisher channel")
    pub = BlockingConnection(config.connection)
    pub_ch = pub.channel()
    pub_ch.confirm_delivery()

    store = Store(config.storage_file, ProcessMap, thread_name="ttq-store")

    publisher = Publisher(
        channel=pub_ch,
        exchange_name=config.response_exchange,
        thread_name="ttq-publisher",
    )

    executor = Executor(
        store=store, publisher=publisher, max_workers=config.max_workers
    )

    prefetch_count = (
        executor.max_workers if config.prefetch_count is None else config.prefetch_count
    )
    logger.debug(f"Setting prefetch count to {prefetch_count}")
    sub_ch.basic_qos(prefetch_count=prefetch_count)

    listener = Listener(
        queue_name=config.request_queue,
        abort_exchange_name=config.request_abort_exchange,
        events=events,
        to_command=to_command,
        store=store,
        executor=executor,
    )
    logger.debug("Binding subscriber channel consumers")
    listener.bind(sub_ch)

    shutdown = Shutdown(
        exchange_name=config.request_stop_exchange,
        routing_key=config.request_stop_routing_key,
        executor=executor,
        store=store,
        publisher=publisher,
    )
    shutdown.bind(sub_ch)

    logger.debug("Starting publisher thread")
    publisher.start()

    logger.debug("Starting process map store")
    store.start()

    try:
        logger.info("Starting to consume messages")
        sub_ch.start_consuming()

    except KeyboardInterrupt:
        trigger_stop(
            publisher, config.request_stop_exchange, config.request_stop_routing_key
        )

    except Exception as e:
        logger.exception(e)
        trigger_stop(
            publisher, config.request_stop_exchange, config.request_stop_routing_key
        )

    finally:
        logger.info("Stopped.")


def trigger_stop(publisher: Publisher, exchange_name: str, routing_key: str):
    logger.warning("Interrupt: sending shutdown message")
    publisher.publish_stop(exchange_name, routing_key)
