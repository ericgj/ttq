import logging
from time import sleep
from threading import Event
from typing import Type, Iterable

root_logger = logging.getLogger()
logger = logging.getLogger(__name__)

from ..adapter.mq.listener import Listener
from ..adapter.mq.handler import Handler
from ..adapter.mq.publisher import Publisher
from ..model.event import EventProtocol, EventHandlerProtocol
from ..model.mq import Queue
from ..model.config import Config


def run(
    config: Config,
    queues: Iterable[Queue],
    events: Iterable[Type[EventProtocol]],
    handlers: Iterable[EventHandlerProtocol],
    stop: Event,
):
    publisher = Publisher(
        connection=config.server,
        queues=queues,
    )

    handler = Handler(
        handlers=handlers,
        publisher=publisher,
        max_workers=config.max_workers
    )

    listener = Listener(
        connection=config.server,
        queue=config.queues.subscribe,
        max=handler.max_workers,
        events=events,
        handle=handler,
    )

    try:
        """TODO
        root_logger.addHandler(
            LoggingHandler(connection=config.server, queue=config.queues.logger)
        )
        """
        listener.start()
        main_loop(listener, stop)

    except Exception as e:
        logger.exception(e)
        raise e

    finally:
        logger.debug("Listener stopping")
        listener.join()
        logger.info("Listener stopped.")
        handler.shutdown()


def main_loop(listener, stop):
    try:
        while not stop.is_set():
            sleep(1)
    except KeyboardInterrupt:
        stop.set()
    finally:
        if stop.is_set():
            listener.stop()














"""

class HandledEventCallback:
    def __init__(
        self,
        *,
        publisher: Publisher,
        context: MessageContext,
        event: EventProtocol,
    ):
        self.publisher = publisher
        self.context = context
        self.event = event

    def __call__(
        self, handler: EventHandlerProtocol, command: Command
    ) -> Callable[[Future], None]:
        ctx = {
            "handler": handler.name,
            "command": command.to_dict(),
            "context": self.context.to_dict(),
            "event": self.event.name,
        }

        def _callback(f: Future):
            try:
                result = f.result()
            except Exception as e:
                logger.warning(
                    f"Error running {command.name} for event handler {handler.name}",
                    ctx,
                )
                logger.exception(e, ctx)
                return

            try:
                resp_queue, resp = handler.response(result)
            except Exception as e:
                logger.warning(
                    f"Error preparing response to {command.name} "
                    f"for event handler {handler.name}",
                    ctx,
                )
                logger.exception(e, ctx)
                return

            if self.context.reply_to is not None:
                # Note: publisher does its own error logging
                self.publisher.publish(
                    resp,
                    routing_key=self.context.reply_to,
                    correlation_id=self.context.correlation_id,
                )

            if resp_queue is not None and not resp_queue == self.context.reply_to:
                # Note: publisher does its own error logging
                self.publisher.publish(
                    resp,
                    routing_key=resp_queue,
                    correlation_id=self.context.correlation_id,
                )

        return _callback

"""
