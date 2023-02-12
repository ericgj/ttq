from typing import Tuple, Optional, TypeVar, cast

try:
    import tomllib
except ImportError:
    import tomli as tomllib

from pika import ConnectionParameters

from ..model.config import Config
from ..util.dict_ import assert_has_field

TOP_LEVEL = "ttq"


def parse_file(file_name: str) -> Tuple[Config, Optional[dict]]:
    with open(file_name, "rb") as f:
        return parse(tomllib.load(f))


def parse(raw: dict) -> Tuple[Config, Optional[dict]]:
    assert_has_field(TOP_LEVEL, "config", raw)
    top = raw[TOP_LEVEL]
    return (
        Config(
            connection=parse_connection(top),
            storage_file=parse_storage_file(top),
            request_queue=parse_request_queue(top),
            request_abort_exchange=parse_request_abort_exchange(top),
            request_stop_exchange=parse_request_stop_exchange(top),
            response_exchange=parse_response_exchange(top),
            response_abort_exchange=parse_response_abort_exchange(top),
            request_stop_routing_key=parse_request_stop_routing_key(top),
            prefetch_count=parse_prefetch_count(top),
            max_workers=parse_max_workers(top),
        ),
        parse_logging(raw),
    )


def parse_connection(raw) -> ConnectionParameters:
    assert_has_field("connection", TOP_LEVEL, raw)
    return ConnectionParameters(**raw["connection"])


def parse_storage_file(raw) -> str:
    return parse_required_string(raw, "storage_file", TOP_LEVEL)


def parse_request_queue(raw) -> str:
    return parse_required_string(raw, "request_queue", TOP_LEVEL)


def parse_request_abort_exchange(raw) -> str:
    s = parse_optional_string(raw, "request_abort_exchange")
    return "" if s is None else s


def parse_request_stop_exchange(raw) -> str:
    return parse_required_string(raw, "request_stop_exchange", TOP_LEVEL)


def parse_response_exchange(raw) -> str:
    s = parse_optional_string(raw, "response_exchange")
    return "" if s is None else s


def parse_response_abort_exchange(raw) -> str:
    s = parse_optional_string(raw, "response_abort_exchange")
    return "" if s is None else s


def parse_request_stop_routing_key(raw) -> str:
    s = parse_optional_string(raw, "request_stop_routing_key")
    return "" if s is None else s


def parse_prefetch_count(raw) -> Optional[int]:
    return parse_optional_int(raw, "prefetch_count")


def parse_max_workers(raw) -> Optional[int]:
    return parse_optional_int(raw, "max_workers")


def parse_logging(raw) -> Optional[dict]:
    v = raw.get("logging", None)
    return None if v is None else strict(dict, "logging", v)


def parse_required_string(raw, key: str, msg: str) -> str:
    assert_has_field(key, msg, raw)
    return strict(str, key, raw[key])


def parse_optional_string(raw, key: str) -> Optional[str]:
    v = raw.get(key, None)
    return None if v is None else strict(str, key, v)


def parse_optional_int(raw, key: str) -> Optional[int]:
    v = raw.get(key, None)
    return None if v is None else strict(int, key, v)


T = TypeVar("T")


def strict(t: T, label: str, value) -> T:
    if not isinstance(value, t):
        raise ValueError(f"Value of {label} is not a {t.__name__}")
    cast(T, value)
    return value
