from dataclasses import dataclass
import json
import logging
import os
import os.path
import shutil
import threading
from typing import Tuple, Optional
from uuid import uuid4

TEST_LOG_DIR = os.path.join(os.path.dirname(__file__), "log")
TEST_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)-1s | %(asctime)s | %(name)s | %(module)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("ttq")

from pika.connection import ConnectionParameters
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import BasicProperties

from ttq.command.run import run
from ttq.model.config import Config
from ttq.model.command import Command
from ttq.model.response import Accepted, Completed
from ttq.util.mapping import compile_type_map

# ------------------------------------------------------------------------------
# TESTS
# ------------------------------------------------------------------------------


def setup_module(_):
    create_test_log_dir()
    create_test_output_dir()
    add_file_logger()


def setup_function(_):
    purge_test_queues()


def create_test_log_dir():
    shutil.rmtree(TEST_LOG_DIR, ignore_errors=True)
    os.mkdir(TEST_LOG_DIR)


def create_test_output_dir():
    shutil.rmtree(TEST_OUTPUT_DIR, ignore_errors=True)
    os.mkdir(TEST_OUTPUT_DIR)


def add_file_logger():
    f = logging.Formatter(
        "%(levelname)-1s | %(asctime)s | %(name)s | %(module)s | %(threadName)s | %(message)s",
    )
    h = logging.FileHandler(os.path.join(TEST_LOG_DIR, "test_command_run.log"))
    h.setFormatter(f)
    logger.addHandler(h)


def purge_test_queues():
    conn = BlockingConnection(TEST_CONFIG.connection)
    ch = conn.channel()
    try:
        ch.queue_purge(TEST_REQUEST_QUEUE)
        ch.queue_purge(TEST_RESPONSE_QUEUE)
    except Exception:
        pass


def test_simple(caplog):

    caplog.set_level(logging.DEBUG, logger="ttq")
    expected_output = "test_command_run_test_simple.txt"

    app = compile_type_map(
        {
            EventTest: EchoCommand(TEST_OUTPUT_DIR),
        }
    )

    # run server (subject under test) in separate thread, shut down after n secs

    server_th = threading.Thread(
        name="test_command_run_server",
        target=run,
        kwargs={
            "config": TEST_CONFIG,
            "app": app,
            "stop": stop_after(1),
        },
    )
    server_th.start()

    # publish test message and expect result using simple RPC client, waiting 1 sec

    client = SimpleRpcClient(
        publish_queue=TEST_REQUEST_QUEUE,
        result_queue=TEST_RESPONSE_QUEUE,
        timeout=1,
    )
    conn = BlockingConnection(TEST_CONFIG.connection)
    was_accepted, result = client(
        conn, f"Hello world!\n{expected_output}", type="EventTest"
    )

    if server_th.is_alive():
        server_th.join()

    assert was_accepted
    assert result == 0
    assert os.path.exists(os.path.join(TEST_OUTPUT_DIR, expected_output))


# ------------------------------------------------------------------------------
# Resources used in tests
# ------------------------------------------------------------------------------


@dataclass
class EventTest:
    type_name = "EventTest"
    content_type = "text/plain"
    encoding: Optional[str]
    payload: str
    output_file: str

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "EventTest":
        s = data.decode() if encoding is None else data.decode(encoding)
        lines = s.split("\n")
        return cls(
            encoding=encoding,
            payload=lines[0],
            output_file=lines[1],
        )

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        s = "\n".join([self.payload, self.output_file])
        return s.encode() if encoding is None else s.encode(encoding)


class EchoCommand:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def __call__(self, event: EventTest) -> Command:
        return Command(
            name="echo",
            args=[
                "echo",
                event.payload,
                ">>",
                os.path.join(self.output_dir, event.output_file),
            ],
            shell=True,
        )


def stop_after(secs: float):
    e = threading.Event()
    t = threading.Timer(secs, lambda: e.set())
    t.start()
    return e


client_logger = logging.getLogger("ttq.test.simple_rpc_client")


class SimpleRpcClient:
    def __init__(
        self,
        *,
        publish_queue: str,
        result_queue: str,
        timeout: float,
    ):
        self.publish_queue = publish_queue
        self.result_queue = result_queue
        self.timeout = timeout
        self.corr_id: Optional[str] = None
        self.accepted_result_received = False
        self.completed_result: Optional[int] = None

    def start(self, conn: BlockingConnection) -> Tuple[BlockingChannel, Optional[str]]:
        channel = conn.channel()

        r = channel.queue_declare(queue=self.result_queue)
        callback_queue = r.method.queue

        channel.basic_consume(
            queue=callback_queue, on_message_callback=self.callback, auto_ack=True
        )

        return (channel, callback_queue)

    def __call__(
        self,
        conn: BlockingConnection,
        msg: str,
        *,
        type: str,
        content_type: str = "text/plain",
        encoding: Optional[str] = None,
    ) -> Tuple[bool, Optional[int]]:
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
                type=type,
            ),
            body=msg.encode() if encoding is None else msg.encode(encoding),
        )
        conn.sleep(self.timeout)  # process events up to self.timeout
        return (self.accepted_result_received, self.completed_result)

    def callback(self, ch, method, props, body):
        try:
            t = props.type
            if t == Accepted.type_name:
                self.handle_accepted_result()
            elif t == Completed.type_name:
                self.handle_completed_result(props, body)
            else:
                raise ValueError(f"Unknown response type {t}")
        except Exception as e:
            logger.error(f"SimpleRpcClient NACK: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def handle_accepted_result(self):
        client_logger.debug("handle_accepted_result")
        if self.accepted_result_received:
            raise ValueError("Received results out of order")
        self.accepted_result_received = True

    def handle_completed_result(self, props, body):
        client_logger.debug("handle_completed_result")
        if not self.accepted_result_received:
            raise ValueError("Received results out of order")
        t = props.type
        ct = props.content_type
        r = body.decode(encoding=props.content_encoding)
        if ct == "text/plain":
            self.completed_result = int(r)
        elif ct == "application/json":
            self.completed_result = json.loads(r)["returncode"]
        else:
            raise ValueError(f"Unknown content type: {ct} for response type {t}")


# ------------------------------------------------------------------------------
# Globals
# ------------------------------------------------------------------------------

TEST_REQUEST_QUEUE = "test_command_run"
TEST_RESPONSE_QUEUE = "test_command_run-response"
TEST_CONFIG = Config(
    connection=ConnectionParameters(host="localhost"),
    subscribe=TEST_REQUEST_QUEUE,
    publish="",
    events=[EventTest],
    storage_file=os.path.join(TEST_OUTPUT_DIR, "process_map"),
)
