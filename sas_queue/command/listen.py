from concurrent.futures import Future
import logging
from time import sleep
from threading import Event
from typing import Type, Iterable, Callable

root_logger = logging.getLogger()
logger = logging.getLogger(__name__)

from ..adapter.executor import CommandExecutor
from ..adapter.mq import EventListener, EventPublisher
from ..model.event import EventProtocol, EventHandlerProtocol
from ..model.mq import MessageContext
from ..model.command import Command
from ..model.config import Config
from ..model.exceptions import EventNotHandled


def run(
    config: Config,
    events: Iterable[Type[EventProtocol]],
    handlers: Iterable[EventHandlerProtocol],
    stop: Event,
):
    executor = CommandExecutor(max_workers=config.max_workers)

    # Note: under assumption that max 25% of jobs could complete concurrently
    publisher = EventPublisher(
        connection=config.server,
        max_workers=max(2, executor.max_workers // 4),
    )

    handler = EventHandlers(
        executor=executor,
        publisher=publisher,
        handlers=handlers,
    )

    listener = EventListener(
        connection=config.server,
        queue=config.queues.subscribe,
        max=executor.max_workers,
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
        logger.debug("Executor stopping")
        executor.shutdown()
        logger.info("Executor stopped.")
        logger.debug("Publisher stopping")
        publisher.shutdown()
        logger.info("Publisher stopped.")


def main_loop(listener, stop):
    try:
        while not stop.is_set():
            sleep(1)
    except KeyboardInterrupt:
        stop.set()
    finally:
        if stop.is_set():
            listener.stop()


class EventHandlers:
    def __init__(
        self,
        *,
        executor: CommandExecutor,
        publisher: EventPublisher,
        handlers: Iterable[EventHandlerProtocol],
    ):
        self.executor = executor
        self.publisher = publisher
        self.handlers = handlers

    def __call__(self, context: MessageContext, event: EventProtocol):
        commands = [
            (h, h(event)) for h in self.handlers if event.content_type in h.event_types
        ]
        if len(commands) == 0:
            raise EventNotHandled(event.content_type)
        self.executor.submit(
            commands,
            HandledEventCallback(
                publisher=self.publisher, context=context, event=event
            ),
        )


class HandledEventCallback:
    def __init__(
        self,
        *,
        publisher: EventPublisher,
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
            if self.context.reply_to is None:
                logger.warning(
                    f"Unable to publish response to {command.name} "
                    f"for event handler {handler.name} "
                    f"because no reply-to specified in source event {self.event.name}",
                    ctx,
                )
                return

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
                resp = handler.response(result)
            except Exception as e:
                logger.warning(
                    f"Error handling response to {command.name} "
                    f"for event handler {handler.name}",
                    ctx,
                )
                logger.exception(e, ctx)
                return

            # Note: publisher does its own error logging
            self.publisher.publish(
                resp,
                routing_key=self.context.reply_to,
                correlation_id=self.context.correlation_id,
            )

        return _callback
