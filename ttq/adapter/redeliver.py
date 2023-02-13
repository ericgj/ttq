from functools import partial
import logging

logger = logging.getLogger(__name__)

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from ..model import message


class Redeliver:
    def __init__(
        self,
        pub_channel: BlockingChannel,
        redeliver: message.Redeliver,
    ):
        self.pub_channel = pub_channel
        self.redeliver = redeliver

    def bind(self, channel: BlockingChannel):
        logger.debug("Declare transient delay queue")
        r = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        queue_name = r.method.queue

        exchange_name = self.redeliver.exchange_name
        routing_key = self.redeliver.routing_key

        logger.debug(
            f"Bind transient delay queue {queue_name} "
            f"to exchange {exchange_name} "
            f"with routing key '{routing_key}'"
        )
        channel.queue_bind(
            exchange=exchange_name,
            queue=queue_name,
            routing_key=routing_key,
        )

        logger.debug(f"Bind message handling on transient delay queue {queue_name}")
        return channel.basic_consume(queue=queue_name, on_message_callback=self._handle)

    def _handle(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        try:
            context = message.Context.from_event(m, p, body)
            redeliver_context = self.redeliver.return_context(context)
            delay = self.redeliver.delay(context)
            thunk = partial(
                self.pub_channel.basic_publish,
                exchange=redeliver_context.exchange_name,
                routing_key=redeliver_context.routing_key,
                properties=redeliver_context.properties,
                body=redeliver_context.body,
            )

            ctx = redeliver_context.to_dict()
            logger.info(
                "Redelivering message "
                f"correlation_id = {redeliver_context.correlation_id} "
                f"after {delay} seconds"
                f"via exchange {redeliver_context.exchange_name} "
                f"with routing key {redeliver_context.routing_key} ",
                ctx,
            )
            self.pub_channel.call_later(delay, thunk)

            ch.basic_ack(m.delivery_tag)

        except Exception as e:
            logger.exception(e)
            ch.basic_nack(m.delivery_tag, requeue=False)
