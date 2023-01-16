from dataclasses import dataclass
import logging
import os
import os.path
import shutil
from subprocess import CompletedProcess
import threading
from typing import Tuple, Optional, Type
from uuid import uuid4

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)-1s | %(asctime)s | %(threadName)s | %(module)s | %(message)s",
)
logger = logging.getLogger(__name__)

from pika.connection import ConnectionParameters
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import BasicProperties

from sas_queue.adapter import mq
from sas_queue.model.mq import Queue
from sas_queue.model.config import Config
from sas_queue.model.event import EventProtocol
from sas_queue.model.command import Command

TEST_INPUT_QUEUE = Queue("test_adapter_mq", accept=frozenset(["text/plain"]))
TEST_OUTPUT_QUEUE = Queue(
    name="test_adapter_mq-result", accept=frozenset(["text/plain"])
)
TEST_CONFIG = Config(
    server=ConnectionParameters(host="localhost"),
    subscribe=TEST_INPUT_QUEUE,
    publish={TEST_OUTPUT_QUEUE},
)
TEST_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


# ------------------------------------------------------------------------------
# TESTS
# ------------------------------------------------------------------------------


def setup_module(_):
    create_test_output_dir()


def setup_function(_):
    purge_test_queues()


def create_test_output_dir():
    shutil.rmtree(TEST_OUTPUT_DIR, ignore_errors=True)
    os.mkdir(TEST_OUTPUT_DIR)


def purge_test_queues():
    conn = BlockingConnection(TEST_CONFIG.server)
    ch = conn.channel()
    try:
        ch.queue_purge(TEST_INPUT_QUEUE.name)
        ch.queue_purge(TEST_OUTPUT_QUEUE.name)
    except Exception:
        pass


def test_simple():

    # run server (subject under test) in separate thread, shut down after n secs

    server_th = threading.Thread(
        name="test_adapter_mq_server",
        target=mq.run,
        kwargs={
            "config": TEST_CONFIG,
            "events": [EventTest],
            "handlers": [EventTestHandler("test_simple")],
            "stop": stop_after(1.1),
        },
    )
    server_th.start()

    # publish test message and expect result using simple RPC client, waiting 1 sec

    client = SimpleRpcClient(
        publish_queue=TEST_INPUT_QUEUE.name,
        result_queue=TEST_OUTPUT_QUEUE.name,
        result_decoder=ResultEventTest,
        timeout=1,
    )
    conn = BlockingConnection(TEST_CONFIG.server)
    result = client(conn, "Hello world!")

    if server_th.is_alive():
        server_th.join()

    assert result is not None
    assert result.returncode == 0


# ------------------------------------------------------------------------------
# EVENT/EVENT HANDLERS used in tests
# ------------------------------------------------------------------------------


@dataclass(frozen=True)
class EventTest:
    name = "test"
    queue = "test_adapter_mq"
    content_type = "text/plain"
    encoding: Optional[str]
    payload: str

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "EventTest":
        return cls(
            encoding=encoding,
            payload=data.decode() if encoding is None else data.decode(encoding),
        )

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        return (
            self.payload.encode() if encoding is None else self.payload.encode(encoding)
        )


@dataclass(frozen=True)
class ResultEventTest:
    name = "test-result"
    queue = "test_adapter_mq-result"
    content_type = "text/plain"
    returncode: int

    @classmethod
    def decode(
        cls, data: bytes, *, encoding: Optional[str] = None
    ) -> "ResultEventTest":
        return cls(
            returncode=int(data.decode() if encoding is None else data.decode(encoding))
        )

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        rc = str(self.returncode)
        return rc.encode() if encoding is None else rc.encode(encoding)


class EventTestHandler:
    name = "test_handler"
    event_types = {EventTest}

    def __init__(self, output_file: str):
        self.output_file = output_file

    def __call__(self, event: EventProtocol) -> Command:
        if isinstance(event, EventTest):
            b_msg = event.encode(encoding=event.encoding)
            s_msg = (
                b_msg.decode()
                if event.encoding is None
                else b_msg.decode(event.encoding)
            )
            return Command(
                name="dump",
                command=[
                    "echo",
                    s_msg,
                    ">>",
                    os.path.join(TEST_OUTPUT_DIR, self.output_file),
                ],
                shell=True,
            )
        raise ValueError(f"Unhandled event type: {type(event)}")

    def response(self, result: CompletedProcess) -> Tuple[Optional[str], EventProtocol]:
        return (None, ResultEventTest(returncode=result.returncode))


# ------------------------------------------------------------------------------
# UTILS - move to separate module?
# ------------------------------------------------------------------------------


def stop_after(secs: float):
    e = threading.Event()
    t = threading.Timer(secs, lambda: e.set())
    t.start()
    return e


class SimpleRpcClient:
    def __init__(
        self,
        *,
        publish_queue: str,
        result_queue: str,
        result_decoder: Type[EventProtocol],
        timeout: Optional[int] = None,
    ):
        self.publish_queue = publish_queue
        self.result_queue = result_queue
        self.result_decoder = result_decoder
        self.timeout = timeout
        self.corr_id: Optional[str] = None
        self.result: Optional[EventProtocol] = None

    def start(self, conn: BlockingConnection) -> Tuple[BlockingChannel, Optional[str]]:
        channel = conn.channel()

        result = channel.queue_declare(queue=self.result_queue)
        callback_queue = result.method.queue

        channel.basic_consume(
            queue=callback_queue, on_message_callback=self.callback, auto_ack=True
        )

        return (channel, callback_queue)

    def __call__(
        self,
        conn: BlockingConnection,
        msg: str,
        *,
        content_type: str = "text/plain",
        encoding: Optional[str] = None,
    ):
        channel, callback_queue = self.start(conn)
        self.corr_id = str(uuid4())
        channel.basic_publish(
            exchange="",
            routing_key=self.publish_queue,
            properties=BasicProperties(
                reply_to=callback_queue,
                correlation_id=self.corr_id,
                content_encoding=encoding,
                content_type=content_type,
            ),
            body=msg.encode() if encoding is None else msg.encode(encoding),
        )
        if self.timeout is None:
            conn.process_data_events()
        else:
            conn.process_data_events(time_limit=self.timeout)
        return self.result

    def callback(self, ch, method, props, body):
        try:
            if self.corr_id == props.correlation_id:
                self.result = self.result_decoder.decode(
                    body, encoding=props.content_encoding
                )
        except Exception as e:
            logger.error(f"NACK: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
