import logging
import threading

from pika.adapters.blocking_connection import BlockingConnection

from ..adapter.listener import Listener, Shutdown
from ..adapter.executor import Executor
from ..adapter.store import Store, ProcessMap
from ..model.config import Config
from ..model import command
from ..util.mapping import compile_type_map

logger = logging.getLogger(__name__)


def run(config: Config, app: command.EventMapping):
    def _send_shutdown_message():
        pub_ch.basic_publish(
            config.request_shutdown_exchange,
            routing_key="",
            body=b"",
        )

    def _exit_handler():
        logger.warning("Interrupt: sending shutdown message")
        pub_ch.add_callback_threadsafe(_send_shutdown_message)

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

    logger.debug("Starting process map store")
    store = Store(config.storage_file, ProcessMap, thread_name="ProcessMap")
    store.start()

    executor = Executor(
        channel=pub_ch,
        exchange_name=config.response_exchange,
        abort_exchange_name=config.response_abort_exchange,
        max_workers=config.max_workers,
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

    shutdown_event = threading.Event()
    shutdown = Shutdown(
        exchange_name=config.request_shutdown_exchange,
        executor=executor,
        store=store,
        stop=shutdown_event,
    )
    shutdown.bind(sub_ch)

    logger.info("Starting to consume messages on subscriber thread")
    sub_th = threading.Thread(name="ttq-subscriber", target=sub_ch.start_consuming)
    sub_th.start()

    logger.info("Starting publisher loop")
    try:
        while not shutdown_event.is_set():
            pub.sleep(0.1)

    except KeyboardInterrupt:
        _exit_handler()

    except Exception as e:
        logger.exception(e)
        raise e

    finally:
        logger.debug("Waiting for publisher channel events to finish")
        pub.process_data_events()

        logger.debug("Waiting for subscriber channel consumers to stop")
        sub.add_callback_threadsafe(sub_ch.stop_consuming)
        sub_th.join()

        logger.info("Stopped.")
