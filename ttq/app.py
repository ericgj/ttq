from functools import wraps
import json
from typing import Callable, Mapping, Optional, TypeVar, Any, Dict

from .model.message import Context
from .model.command import Command
from .model.exceptions import MessageNotHandled
from .util.validate import dict_field

App = Callable[[Context], Command]

A = TypeVar("A")

"""
Utilities for applications
"""


def compile(
    mapping: Mapping[str, App],
) -> App:
    """
    Converts a mapping of type name to App into a single App
    """

    @wraps(compile)
    def _compile(c: Context) -> Command:
        if c.type is not None and c.type in mapping:
            return mapping[c.type](c)
        else:
            raise MessageNotHandled(c.type, c.content_type)

    return _compile


def decode(
    content_types: set[str], decoder: Callable[[Optional[str], bytes], A]
) -> Callable[[Callable[[A], Command]], App]:
    """
    Decorator to decode messages of given content types before converting to Command
    """

    @wraps(decode)
    def _decode(fn: Callable[[A], Command]) -> App:
        def __decode(c: Context) -> Command:
            if c.content_type is not None and c.content_type in content_types:
                return fn(decoder(c.content_encoding, c.content))
            else:
                raise MessageNotHandled(c.type, c.content_type)

        return __decode

    return _decode


def as_string(encoding: Optional[str], data: bytes) -> str:
    """Decode as string"""
    return data.decode() if encoding is None else data.decode(encoding)


def as_json(encoding: Optional[str], data: bytes) -> Dict[str, Any]:
    """Decode as JSON dict"""
    return dict_field("_", {"_": json.loads(as_string(encoding, data))})
