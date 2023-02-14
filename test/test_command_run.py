import logging
import os.path
import os
from queue import Queue
import shutil
import threading
from typing import Any, Iterable, List, Optional, Callable, Dict
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
from pika.exchange_type import ExchangeType

# import pytest  # type: ignore

from ttq.command.run import run
from ttq.model.config import Config
from ttq.model.event import EventProtocol
from ttq.model.command import Command, EventMapping

from util.script import Script, ScriptHandlerProtocol, wait  # , send
from util.expect import Expect, that, evaluate
from util.queue import queue_iterator
from util.pytest import assert_no_log_matching, assert_logs_matching_in_order


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


# @pytest.mark.skip()
def test_run_success(caplog):
    caplog.set_level(logging.DEBUG, logger="ttq")
    caplog.set_level(logging.DEBUG, logger=__name__)

    dur = 2
    script = wait(0.5).and_send(SleepEvent(dur)).and_wait(1)

    expected: List[Expect[Response]] = [
        that(is_accepted_response) & ~that(is_aborted_response),
        that(is_started_response) & ~that(is_aborted_response),
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
        check=check_has_accepted_started_completed_triplets,
        app=app,
        id_field="correlation_id",
    )


# @pytest.mark.skip()
def test_run_and_abort_success(caplog):
    caplog.set_level(logging.DEBUG, logger="ttq")
    caplog.set_level(logging.DEBUG, logger=__name__)

    dur = 3
    script = wait(0.5).and_send(SleepEvent(dur)).and_wait(0.5).and_abort(-1).and_wait(1)

    that_sleep_accepted = that(is_accepted_response) & ~that(is_aborted_response)
    that_sleep_started = that(is_started_response) & ~that(is_aborted_response)
    that_sleep_failed = (
        that(is_completed_response)
        & ~that(is_aborted_response)
        & that(is_error_response)
    )
    that_abort_accepted = that(is_accepted_response) & that(is_aborted_response)
    that_abort_started = that(is_started_response) & that(is_aborted_response)
    that_abort_completed = that(is_completed_response) & that(is_aborted_response)

    # Note: completed responses can come back in either order.
    expected: List[Expect[Response]] = [
        that_sleep_accepted,
        that_sleep_started,
        that_abort_accepted,
        that_abort_started,
        that_sleep_failed | that_abort_completed,
        that_sleep_failed | that_abort_completed,
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
        check=check_has_accepted_started_completed_triplets,
        app=app,
        id_field="message_id",
    )


# @pytest.mark.skip()
def test_run_many_success(caplog):
    caplog.set_level(logging.DEBUG, logger="ttq")
    caplog.set_level(logging.DEBUG, logger=__name__)

    dur = 2
    times = 10
    every = 0.5
    script = (
        wait(0.5)
        .and_send_repeatedly(lambda i: SleepEvent(dur), times, every)
        .and_wait(1)
    )

    expected: List[Expect[Response]] = [
        ~that(is_error_response)
        & (
            that(is_accepted_response)
            | that(is_started_response)
            | that(is_completed_response)
        ),
    ] * (times * 3)

    config = TestingConfig(
        name="test_command_run",
        temp_dir=output_dir("test_run_many_success"),
    )

    app = {SleepEvent: SleepCommand()}

    run_script_and_evaluate(
        config=config,
        script=script,
        expected=expected,
        check=check_has_accepted_started_completed_triplets,
        app=app,
        id_field="message_id",
    )


# @pytest.mark.skip()
def test_run_with_redeliver(caplog):
    caplog.set_level(logging.DEBUG, logger="ttq")
    caplog.set_level(logging.DEBUG, logger=__name__)

    dur = 2
    times = 2
    every = 0.1
    script = wait(0.1).and_send_repeatedly(lambda i: SleepEvent(dur), times, every)

    config = TestingConfig(
        name="test_command_run",
        temp_dir=output_dir("test_run_with_redeliver"),
        max_workers=1,
    )

    app = {SleepEvent: SleepCommand()}

    run_script(
        config=config,
        script=script,
        app=app,
        id_field="message_id",
    )

    # Expect that
    # 1) the delay queue is set up,
    # 2) second task is cancelled on shutdown,
    # 3) message is sent to the delay queue,
    # 4) but not handled by Redeliver handler before shutdown.
    #
    assert_logs_matching_in_order(
        [
            (r"Bind transient delay queue", None),
            (
                r"During shutdown, command ping for correlation_id [a-f0-9\-]+ was cancelled",
                logging.WARNING,
            ),
            (r"Redelivering message to delay queue via exchange", None),
        ],
        caplog.records,
    )
    assert_no_log_matching(
        r"Redelivering message correlation_id [a-f0-9\-]+ after \d+ seconds",
        caplog.records,
    )


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


class TestingConfig:
    __test__ = False

    def __init__(self, *, name: str, temp_dir: str, max_workers: int = 10):
        self.name = name
        self.temp_dir = temp_dir
        self.max_workers = max_workers

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
    def request_abort_exchange(self) -> str:
        return f"{self.name}-abort-req-x"

    @property
    def response_abort_exchange(self) -> str:
        return f"{self.name}-abort-resp-x"

    @property
    def response_abort_queue(self) -> str:
        return f"{self.name}-abort-resp"

    @property
    def request_stop_exchange(self) -> str:
        return f"{self.name}-stop-req-x"

    @property
    def redeliver_exchange(self) -> str:
        return f"{self.name}-redeliver-x"

    @property
    def redeliver_routing_key(self) -> str:
        return ""

    @property
    def connection_parameters(self) -> ConnectionParameters:
        return ConnectionParameters(host="localhost")

    @property
    def ttq(self) -> Config:
        return Config(
            connection=self.connection_parameters,
            request_queue=self.request_queue,
            request_abort_exchange=self.request_abort_exchange,
            request_stop_exchange=self.request_stop_exchange,
            response_exchange=self.response_exchange,
            response_abort_exchange=self.response_abort_exchange,
            redeliver_exchange=self.redeliver_exchange,
            redeliver_routing_key=self.redeliver_routing_key,
            storage_file=os.path.join(self.temp_dir, "process_map"),
            prefetch_count=1,
            max_workers=self.max_workers,
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
    id_field: str,
    check: Optional[Callable[[List[Response]], Optional[str]]] = None,
):
    results = run_script(
        config=config,
        script=script,
        app=app,
        id_field=id_field,
    )
    evaluate(expected, results, check_all=check)


def run_script(
    config: TestingConfig,
    script: Script,
    app: EventMapping,
    id_field: str,
) -> Iterable[Response]:
    logger.debug("connect and bind request channel")
    request_ch = connect_and_bind_request_channel(config)
    logger.debug("connect and bind response channel")
    resp_ch = connect_and_bind_response_channel(config)

    resp_q: "Queue[Response]" = Queue()
    relay = Relay(
        response_queue=config.response_queue,
        response_abort_queue=config.response_abort_queue,
        local_queue=resp_q,
    )
    relay.bind(resp_ch)

    publisher = ScriptPublisher(
        channel=request_ch,
        request_exchange=config.request_exchange,
        request_queue=config.request_queue,
        request_abort_exchange=config.request_abort_exchange,
        response_queue=config.response_queue,
        response_abort_queue=config.response_abort_queue,
        request_stop_exchange=config.request_stop_exchange,
        id_field=id_field,
        finish=lambda: resp_ch.connection.call_later(2, resp_ch.stop_consuming),
    )

    ttq_th = run_ttq_in_thread(
        name="ttq",
        config=config.ttq,
        app=app,
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

    # timer.start()

    logger.debug("listening for responses")
    resp_ch.start_consuming()

    logger.debug("done, joining script thread")
    script_th.join()

    logger.debug("done, joining ttq thread")
    ttq_th.join()

    return queue_iterator(resp_q)


def is_accepted_response(r: Response) -> bool:
    return r.type_name == "Accepted"


def is_rejected_response(r: Response) -> bool:
    return r.type_name == "Rejected"


def is_started_response(r: Response) -> bool:
    return r.type_name == "Started"


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


def check_has_accepted_started_completed_triplets(rs: List[Response]) -> Optional[str]:
    accepted_ids = set(
        [
            r.properties.correlation_id
            for r in rs
            if is_accepted_response(r) and r.properties.correlation_id is not None
        ]
    )
    started_ids = set(
        [
            r.properties.correlation_id
            for r in rs
            if is_started_response(r) and r.properties.correlation_id is not None
        ]
    )
    completed_ids = set(
        [
            r.properties.correlation_id
            for r in rs
            if is_completed_response(r) and r.properties.correlation_id is not None
        ]
    )
    accepted_not_started = accepted_ids - started_ids
    started_not_accepted = started_ids - accepted_ids
    started_not_completed = started_ids - completed_ids
    completed_not_started = completed_ids - started_ids
    completed_not_accepted = completed_ids - accepted_ids
    accepted_not_completed = accepted_ids - completed_ids

    if (
        len(accepted_not_started) == 0
        and len(started_not_accepted) == 0
        and len(started_not_completed) == 0
        and len(completed_not_started) == 0
        and len(completed_not_accepted) == 0
        and len(accepted_not_completed) == 0
    ):
        return None
    else:
        return ". ".join(
            (
                []
                if len(started_not_completed) == 0 and len(accepted_not_completed) == 0
                else [
                    "Missing completed responses for the following correlation_ids: "
                    + ", ".join(list(started_not_completed | accepted_not_completed))
                ]
            )
            + (
                []
                if len(accepted_not_started) == 0 and len(completed_not_started) == 0
                else [
                    "Missing started responses for the following correlation_ids: "
                    + ", ".join(list(accepted_not_started | completed_not_started))
                ]
            )
            + (
                []
                if len(started_not_accepted) == 0 and len(completed_not_accepted) == 0
                else [
                    "Missing accepted responses for the following correlation_ids: "
                    + ", ".join(list(started_not_accepted | completed_not_accepted))
                ]
            )
        )


def connect_and_bind_request_channel(config: TestingConfig) -> BlockingChannel:
    c = connect(config)
    ch = c.channel()
    ch.confirm_delivery()

    # Note: abort request exchange bound on the fly to transient queues, so
    # not bound here.

    args = {"x-message-ttl": 1000}
    ch.queue_declare(config.request_queue, auto_delete=True, arguments=args)

    if not config.request_exchange == "":
        ch.exchange_declare(config.request_exchange, auto_delete=True)
        ch.queue_bind(config.request_queue, config.request_exchange)

    if not config.request_abort_exchange == "":
        ch.exchange_declare(config.request_abort_exchange, auto_delete=True)

    if not config.redeliver_exchange == "":
        ch.exchange_declare(config.redeliver_exchange, auto_delete=True)

    ch.exchange_declare(
        config.request_stop_exchange,
        exchange_type=ExchangeType.fanout,
        auto_delete=True,
    )

    return ch


def connect_and_bind_response_channel(config: TestingConfig) -> BlockingChannel:
    c = connect(config)
    ch = c.channel()
    ch.confirm_delivery()

    args = {"x-message-ttl": 1000}
    ch.queue_declare(config.response_queue, auto_delete=True, arguments=args)
    ch.queue_declare(config.response_abort_queue, auto_delete=True, arguments=args)

    if not config.response_exchange == "":
        ch.exchange_declare(config.response_exchange, auto_delete=True)
        ch.queue_bind(config.response_queue, config.response_exchange)

    if not config.response_abort_exchange == "":
        ch.exchange_declare(config.response_abort_exchange, auto_delete=True)
        ch.queue_bind(config.response_abort_queue, config.response_abort_exchange)

    return ch


def connect(config: TestingConfig) -> BlockingConnection:
    return BlockingConnection(config.connection_parameters)


def run_ttq_in_thread(
    name: str,
    config: Config,
    app: EventMapping,
):
    return threading.Thread(
        name=name,
        target=run,
        kwargs={
            "config": config,
            "app": app,
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
        response_abort_queue: str,
        local_queue: "Queue[Response]",
    ):
        self.response_queue = response_queue
        self.response_abort_queue = response_abort_queue
        self.local_queue = local_queue

    def bind(self, ch: BlockingChannel):
        ch.basic_consume(self.response_queue, self._handle, auto_ack=True)
        ch.basic_consume(self.response_abort_queue, self._handle_abort, auto_ack=True)

    def _handle(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        r = Response(p, body)
        logger.debug(f"Received: {r}")
        self.local_queue.put(r)

    def _handle_abort(
        self, ch: BlockingChannel, m: Basic.Deliver, p: BasicProperties, body: bytes
    ):
        r = AbortResponse(p, body)
        logger.debug(f"Received: {r}")
        self.local_queue.put(r)


class ScriptPublisher:
    def __init__(
        self,
        channel: BlockingChannel,
        *,
        request_exchange: str,
        request_queue: str,
        request_abort_exchange: str,
        request_stop_exchange: str,
        response_queue: str,
        response_abort_queue: str,
        id_field: str,
        finish: Callable[[], Any],
    ):
        self.channel = channel
        self.request_exchange = request_exchange
        self.request_queue = request_queue
        self.request_abort_exchange = request_abort_exchange
        self.request_stop_exchange = request_stop_exchange
        self.response_queue = response_queue
        self.response_abort_queue = response_abort_queue
        self.id_field = id_field
        self._finish = finish

    def send(self, event: EventProtocol) -> str:
        id = str(uuid4())
        logger.debug(
            f"Publishing event {event} to {self.request_exchange}, "
            f"type: {event.type_name}, "
            f"routing_key: {self.request_queue}, "
            f"reply_to: {self.response_queue}, "
            f"{self.id_field}: {id}"
        )
        props: Dict[str, Any] = {
            "reply_to": self.response_queue,
            self.id_field: id,
            "type": event.type_name,
            "content_type": "text/plain",
        }
        self.channel.basic_publish(
            exchange=self.request_exchange,
            routing_key=self.request_queue,
            properties=BasicProperties(**props),
            body=event.encode(),
        )
        return id

    def abort(self, routing_key: str):
        id = str(uuid4())
        logger.debug(
            f"Publishing abort to {self.request_abort_exchange}, "
            f"routing_key: {routing_key}, "
            f"reply_to: {self.response_abort_queue}, "
            f"{self.id_field}: {id}"
        )
        props: Dict[str, Any] = {
            "reply_to": self.response_abort_queue,
            self.id_field: id,
            "content_type": "text/plain",
        }
        self.channel.basic_publish(
            exchange=self.request_abort_exchange,
            routing_key=routing_key,
            properties=BasicProperties(**props),
            body=b"",
        )

    def finish(self, keys: List[str]):
        logger.debug(f"Publishing stop to {self.request_stop_exchange}")
        self.channel.basic_publish(
            exchange=self.request_stop_exchange,
            routing_key="",
            body=b"",
        )
        self._finish()
