import logging
import os.path
import os
from queue import Queue
import shutil
import threading
from typing import List, Optional
from uuid import uuid4

OUTPUT_DIR = os.path.join("test", "output", "test_command_run")
LOG_DIR = os.path.join("test", "log")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)-1s | %(asctime)s | %(name)s | %(module)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger(__name__)

from pika.connection import ConnectionParameters
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties

from ttq.command.run import run
from ttq.model.config import Config
from ttq.model.event import EventProtocol
from ttq.model.command import Command, EventMapping

from util.script import Script, ScriptHandlerProtocol, send
from util.expect import Expect, that, evaluate
from util.queue import queue_iterator


def setup_module(_):
    add_file_logger()


def setup_function(f):
    create_test_output_dir(f.__name__)


def create_test_output_dir(name):
    dir = os.path.join(OUTPUT_DIR, name)
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir, exist_ok=True)


def add_file_logger():
    f = logging.Formatter(
        "%(levelname)-1s | %(asctime)s | %(name)s | %(module)s | %(threadName)s | %(message)s",
    )
    h = logging.FileHandler(os.path.join(LOG_DIR, "test_command_run.log"), mode="w")
    h.setFormatter(f)
    base_logger = logging.getLogger()
    base_logger.addHandler(h)


def output_dir(test_name: str):
    return os.path.join(OUTPUT_DIR, test_name)


def test_run_success(caplog):
    caplog.set_level(logging.DEBUG, logger="ttq")
    caplog.set_level(logging.DEBUG, logger=__name__)

    dur = 2
    script = send(SleepEvent(dur))

    expected: List[Expect[Response]] = [
        that(is_accepted_response) & ~that(is_aborted_response),
        that(is_completed_response)
        & ~that(is_aborted_response)
        & ~that(is_error_response),
    ]

    config = TestingConfig(
        name="test_command_run",
        temp_dir=output_dir("test_run_success"),
    )

    app = {SleepEvent: SleepCommand()}

    run_script_and_evaluate(
        config=config,
        script=script,
        expected=expected,
        app=app,
        stop_after=dur + 1,
    )


def test_run_and_abort_success(caplog):
    """
    Note this *usually* passes. But sometimes the 3rd and 4th response
    come back in reverse order from expected.
    """

    caplog.set_level(logging.DEBUG, logger="ttq")
    caplog.set_level(logging.DEBUG, logger=__name__)

    dur = 3
    script = send(SleepEvent(dur)).and_wait(1).and_abort(-1)

    expected: List[Expect[Response]] = [
        that(is_accepted_response) & ~that(is_aborted_response),
        that(is_accepted_response) & that(is_aborted_response),
        that(is_completed_response)
        & ~that(is_aborted_response)
        & that(is_error_response),
        that(is_completed_response) & that(is_aborted_response),
    ]

    config = TestingConfig(
        name="test_command_run_and_abort",
        temp_dir=output_dir("test_run_and_abort_success"),
    )

    app = {SleepEvent: SleepCommand()}

    run_script_and_evaluate(
        config=config,
        script=script,
        expected=expected,
        app=app,
        stop_after=dur + 1,
    )


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


class TestingConfig:
    __test__ = False

    def __init__(self, *, name: str, temp_dir: str):
        self.name = name
        self.temp_dir = temp_dir

    @property
    def request_exchange(self) -> str:
        return f"{self.name}-req-x"

    @property
    def request_queue(self) -> str:
        return f"{self.name}-req"

    @property
    def response_exchange(self) -> str:
        return ""

    @property
    def response_queue(self) -> str:
        return f"{self.name}-resp"

    @property
    def abort_request_exchange(self) -> str:
        return f"{self.name}-abort-req-x"

    @property
    def abort_response_exchange(self) -> str:
        return f"{self.name}-abort-resp-x"

    @property
    def abort_response_queue(self) -> str:
        return f"{self.name}-abort-resp"

    @property
    def connection_parameters(self) -> ConnectionParameters:
        return ConnectionParameters(host="localhost")

    @property
    def ttq(self) -> Config:
        return Config(
            connection=self.connection_parameters,
            subscribe_queue=self.request_queue,
            subscribe_abort_exchange=self.abort_request_exchange,
            storage_file=os.path.join(self.temp_dir, "process_map"),
            prefetch_count=1,
        )


class Response:
    def __init__(self, properties: BasicProperties, body: bytes):
        self.properties = properties
        self.body = body

    @property
    def type_name(self) -> Optional[str]:
        return self.properties.type

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(properties={self.properties})"


class AbortResponse(Response):
    pass


def run_script_and_evaluate(
    config: TestingConfig,
    script: Script,
    expected: List[Expect[Response]],
    app: EventMapping,
    stop_after: float,
):
    logger.debug("connect and bind request channel")
    request_ch = connect_and_bind_request_channel(config)
    logger.debug("connect and bind response channel")
    resp_ch = connect_and_bind_response_channel(config)

    resp_q: "Queue[Response]" = Queue()
    relay = Relay(
        response_queue=config.response_queue,
        abort_response_queue=config.abort_response_queue,
        local_queue=resp_q,
    )
    relay.bind(resp_ch)

    publisher = ScriptPublisher(
        channel=request_ch,
        request_exchange=config.request_exchange,
        request_queue=config.request_queue,
        abort_request_exchange=config.abort_request_exchange,
        response_queue=config.response_queue,
        abort_response_queue=config.abort_response_queue,
    )

    stop = threading.Event()
    timer = threading.Timer(stop_after, stop.set)

    ttq_th = run_ttq_in_thread(
        name="ttq",
        config=config.ttq,
        app=app,
        stop=stop,
    )
    script_th = run_script_in_thread(
        name="script",
        script=script,
        handler=publisher,
    )

    logger.debug("start running ttq in thread")
    ttq_th.start()

    logger.debug("start publishing events in thread")
    script_th.start()

    timer.start()

    logger.debug("listening for responses")
    while not stop.is_set():
        resp_ch.connection.sleep(0.1)

    logger.debug("timer set, joining script thread")
    script_th.join()

    logger.debug("timer set, joining ttq thread")
    ttq_th.join()

    evaluate(expected, queue_iterator(resp_q))


def is_accepted_response(r: Response) -> bool:
    return r.type_name == "Accepted"


def is_completed_response(r: Response) -> bool:
    return r.type_name == "Completed"


def is_aborted_response(r: Response) -> bool:
    return isinstance(r, AbortResponse)


def is_success_response(r: Response) -> bool:
    if r.type_name == "Completed":
        rc = int(r.body.decode())  # quick n dirty
        return rc == 0
    return False


def is_error_response(r: Response) -> bool:
    if r.type_name == "Completed":
        rc = int(r.body.decode())  # quick n dirty
        return rc != 0
    return False


def connect_and_bind_request_channel(config: TestingConfig) -> BlockingChannel:
    c = connect(config)
    ch = c.channel()

    # Note: abort request exchange bound on the fly to transient queues, so
    # not bound here.

    ch.queue_declare(config.request_queue, auto_delete=True)

    if not config.request_exchange == "":
        ch.exchange_declare(config.request_exchange, auto_delete=True)
        ch.queue_bind(config.request_queue, config.request_exchange)

    if not config.abort_request_exchange == "":
        ch.exchange_declare(config.abort_request_exchange, auto_delete=True)

    return ch


def connect_and_bind_response_channel(config: TestingConfig) -> BlockingChannel:
    c = connect(config)
    ch = c.channel()

    ch.queue_declare(config.response_queue, auto_delete=True)
    ch.queue_declare(config.abort_response_queue, auto_delete=True)

    if not config.response_exchange == "":
        ch.exchange_declare(config.response_exchange, auto_delete=True)
        ch.queue_bind(config.response_queue, config.response_exchange)

    if not config.abort_response_exchange == "":
        ch.exchange_declare(config.abort_response_exchange, auto_delete=True)
        ch.queue_bind(config.abort_response_queue, config.abort_response_exchange)

    return ch


def connect(config: TestingConfig) -> BlockingConnection:
    return BlockingConnection(config.connection_parameters)


def run_ttq_in_thread(
    name: str,
    config: Config,
    app: EventMapping,
    stop: threading.Event,
):
    return threading.Thread(
        name=name,
        target=run,
        kwargs={
            "config": config,
            "app": app,
            "stop": stop,
        },
    )


def run_script_in_thread(
    name: str,
    script: Script,
    handler: ScriptHandlerProtocol,
):
    return threading.Thread(
        name=name,
        target=script.run,
        args=[handler],
    )


class SleepEvent:
    type_name = "SleepEvent"
    content_type = "text/plain"

    def __init__(self, duration: float):
        self.duration = duration

    @classmethod
    def decode(cls, data: bytes, *, encoding: Optional[str] = None) -> "SleepEvent":
        s = data.decode() if encoding is None else data.decode(encoding=encoding)
        return cls(float(s))

    def encode(self, *, encoding: Optional[str] = None) -> bytes:
        s = str(self.duration)
        return s.encode() if encoding is None else s.encode(encoding=encoding)


class SleepCommand:
    def __call__(self, event: SleepEvent) -> Command:
        n = int(event.duration + 1)
        return Command("ping", ["ping", "-n", str(n), "127.0.0.1"])


class Relay:
    def __init__(
        self,
        *,
        response_queue: str,
        abort_response_queue: str,
        local_queue: "Queue[Response]",
    ):
        self.response_queue = response_queue
        self.abort_response_queue = abort_response_queue
        self.local_queue = local_queue

    def bind(self, ch: BlockingChannel):
        ch.basic_consume(self.response_queue, self._handle)
        ch.basic_consume(self.abort_response_queue, self._handle_abort)

    def _handle(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        r = Response(p, body)
        logger.debug(f"Received: {r}")
        self.local_queue.put(r)
        if m.delivery_tag is not None:
            ch.basic_ack(m.delivery_tag)

    def _handle_abort(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        r = AbortResponse(p, body)
        logger.debug(f"Received: {r}")
        self.local_queue.put(r)
        if m.delivery_tag is not None:
            ch.basic_ack(m.delivery_tag)


class ScriptPublisher:
    def __init__(
        self,
        channel: BlockingChannel,
        *,
        request_exchange: str,
        request_queue: str,
        abort_request_exchange: str,
        response_queue: str,
        abort_response_queue: str,
    ):
        self.channel = channel
        self.request_exchange = request_exchange
        self.request_queue = request_queue
        self.abort_request_exchange = abort_request_exchange
        self.response_queue = response_queue
        self.abort_response_queue = abort_response_queue

    def send(self, event: EventProtocol) -> str:
        corr_id = str(uuid4())
        logger.debug(
            f"Publishing event {event} to {self.request_exchange}, "
            f"type: {event.type_name}, "
            f"routing_key: {self.request_queue}, "
            f"reply_to: {self.response_queue}, "
            f"correlation_id: {corr_id}"
        )
        self.channel.basic_publish(
            exchange=self.request_exchange,
            routing_key=self.request_queue,
            properties=BasicProperties(
                reply_to=self.response_queue,
                correlation_id=corr_id,
                type=event.type_name,
                content_type="text/plain",
            ),
            body=event.encode(),
        )
        return corr_id

    def abort(self, routing_key: str):
        corr_id = str(uuid4())
        logger.debug(
            f"Publishing abort to {self.abort_request_exchange}, "
            f"routing_key: {routing_key}, "
            f"reply_to: {self.abort_response_queue}, "
            f"correlation_id: {corr_id}"
        )
        self.channel.basic_publish(
            exchange=self.abort_request_exchange,
            routing_key=routing_key,
            properties=BasicProperties(
                reply_to=self.abort_response_queue,
                correlation_id=corr_id,
                content_type="text/plain",
            ),
            body=b"",
        )
