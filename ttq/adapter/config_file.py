import tomllib
from typing import Dict, Tuple, Optional, Any

from pika import ConnectionParameters

from ..model.config import Config
from ..util.validate import (
    dict_field,
    str_field,
    optional_str_field,
    optional_int_field,
)

TOP_LEVEL = "ttq"


def parse_file(file_name: str) -> Tuple[Config, Optional[Dict[str, Any]]]:
    with open(file_name, "rb") as f:
        return parse(tomllib.load(f))


def parse(raw: Dict[str, Any]) -> Tuple[Config, Optional[Dict[str, Any]]]:
    top = dict_field(TOP_LEVEL, raw)
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
            redeliver_exchange=parse_redeliver_exchange(top),
            redeliver_routing_key=parse_redeliver_routing_key(top),
            redeliver_limit=parse_redeliver_limit(top),
            prefetch_count=parse_prefetch_count(top),
            max_workers=parse_max_workers(top),
        ),
        parse_logging(raw),
    )


def parse_connection(raw: Dict[str, Any]) -> ConnectionParameters:
    return ConnectionParameters(**dict_field("connection", raw))


def parse_storage_file(raw: Dict[str, Any]) -> str:
    return str_field("storage_file", raw)


def parse_request_queue(raw: Dict[str, Any]) -> str:
    return str_field("request_queue", raw)


def parse_request_abort_exchange(raw: Dict[str, Any]) -> str:
    s = optional_str_field("request_abort_exchange", raw)
    return "" if s is None else s


def parse_request_stop_exchange(raw: Dict[str, Any]) -> str:
    return str_field("request_stop_exchange", raw)


def parse_response_exchange(raw: Dict[str, Any]) -> str:
    s = optional_str_field("response_exchange", raw)
    return "" if s is None else s


def parse_response_abort_exchange(raw: Dict[str, Any]) -> str:
    s = optional_str_field("response_abort_exchange", raw)
    return "" if s is None else s


def parse_request_stop_routing_key(raw: Dict[str, Any]) -> str:
    s = optional_str_field("request_stop_routing_key", raw)
    return "" if s is None else s


def parse_redeliver_exchange(raw: Dict[str, Any]) -> str:
    s = optional_str_field("redeliver_exchange", raw)
    return "" if s is None else s


def parse_redeliver_routing_key(raw: Dict[str, Any]) -> str:
    s = optional_str_field("redeliver_routing_key", raw)
    return "" if s is None else s


def parse_redeliver_limit(raw: Dict[str, Any]) -> Optional[int]:
    return optional_int_field("redeliver_limit", raw)


def parse_prefetch_count(raw: Dict[str, Any]) -> Optional[int]:
    return optional_int_field("prefetch_count", raw)


def parse_max_workers(raw: Dict[str, Any]) -> Optional[int]:
    return optional_int_field("max_workers", raw)


def parse_logging(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if "logging" not in raw:
        return None
    return dict_field("logging", raw)
