from concurrent.futures import ThreadPoolExecutor
import logging
from subprocess import CompletedProcess
from typing import Optional, Iterable, List, Tuple

from pika import ConnectionParameters

logger = logging.getLogger(__name__)

from ...adapter.mq.publisher import Publisher
from ...adapter import subprocess_
from ...model.mq import MessageContext, Queue
from ...model.event import EventProtocol, EventHandlerProtocol
from ...model.command import Command
from ...model.exceptions import (
    EventNotHandled,
)
from ...util.concurrent.futures import log_exceptions


class Handler:
    def __init__(
        self,
        *,
        connection: ConnectionParameters,
        queues: Iterable[Queue],
        handlers: Iterable[EventHandlerProtocol],
        max_workers: Optional[int] = None,
    ):
        self.connection = connection
        self.queues = queues
        self.handlers = handlers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    @property
    def max_workers(self) -> int:
        return self._executor._max_workers

    def __call__(self, context: MessageContext, event: EventProtocol):
        handler_commands = self.handler_commands_for(event)
        if len(handler_commands) == 0:
            raise EventNotHandled(event.name)
        f_map = {
            self._executor.submit(self.run_and_publish, context, h, c): (h, c)
            for (h, c) in handler_commands
        }
        for f in f_map:
            h, c = f_map[f]
            ctx = {
                "handler": h.name,
                "command": c.to_dict(),
                "context": context.to_dict(),
            }
            f.add_done_callback(
                log_exceptions(
                    message=f"Error handling command {c.name} for handler {h.name}",
                    logger=logger,
                    context=ctx,
                )
            )

    def handler_commands_for(self, event) -> List[Tuple[EventHandlerProtocol, Command]]:
        return [
            (h, h(event))
            for h in self.handlers
            if isinstance(event, tuple(h.event_types))
        ]

    def run_and_publish(
        self, context: MessageContext, handler: EventHandlerProtocol, command: Command
    ):
        """Note: run in thread"""
        ctx = {
            "handler": handler.name,
            "command": command.to_dict(),
            "context": context.to_dict(),
        }
        logger.debug(f"Running command {command.name} for hander {handler.name}", ctx)
        result = self.run(command)
        logger.info(
            f"Ran command {command.name} for hander {handler.name}: "
            f"rc = {result.returncode}",
            ctx,
        )

        logger.debug(f"Handler {handler.name} constructing response event", ctx)
        resp_queue, resp = handler.response(result)
        logger.debug(
            f"Handler {handler.name} constructed response event: "
            f"queue = {resp_queue}, event = {resp.name}",
            ctx,
        )

        pub = self.publisher()
        if context.reply_to is not None:
            logger.debug(
                f"Publishing {resp.name} to reply_to queue {context.reply_to}", ctx
            )
            pub.publish(
                resp,
                routing_key=context.reply_to,
                correlation_id=context.correlation_id,
            )
            logger.info(
                f"Published {resp.name} to reply_to queue {context.reply_to}", ctx
            )

        if resp_queue is not None and not resp_queue == context.reply_to:
            logger.debug(f"Publishing {resp.name} to queue {resp_queue}", ctx)
            pub.publish(
                resp,
                routing_key=resp_queue,
                correlation_id=context.correlation_id,
            )
            logger.debug(f"Published {resp.name} to queue {resp_queue}", ctx)

    def run(self, command: Command) -> CompletedProcess:
        return subprocess_.run(command)

    def publisher(self) -> Publisher:
        return Publisher(self.connection, self.queues)

    def shutdown(self):
        logger.debug("Shutting down executor")
        self._executor.shutdown(wait=True)
        logger.info("Shut down executor.")
