from functools import wraps
from typing import TypeVar, Type, Callable, Mapping

A = TypeVar("A")
B = TypeVar("B")


def compile_type_map(map: Mapping[Type[A], Callable[[A], B]]) -> Callable[[A], B]:
    @wraps(compile_type_map)
    def _compiled(a: A) -> B:
        try:
            return next(map[k](a) for k in map if isinstance(a, k))
        except StopIteration:
            pass
        raise KeyError(a.__class__)

    return _compiled
