from typing import TypeVar, Any, Callable

A = TypeVar("A")
B = TypeVar("B")


def value_of(t: type[A], value: Any) -> A:
    assert isinstance(value, t), f"Expected value to be {repr(t)}: {value}"
    ret: A = value
    return ret


def field_of(t: type[A], key: str, data: dict[str, Any]) -> A:
    v = required_field(key, data)
    assert isinstance(v, t), f"Expected '{key}' value to be {repr(t)}: {data}"
    ret: A = v
    return ret


def optional_field_of(t: type[A], key: str, data: dict[str, Any]) -> A | None:
    if data.get(key, None) is None:
        return None
    else:
        return field_of(t, key, data)


def required_field(key: str, data: dict[str, Any]) -> Any:
    if key not in data:
        raise KeyError(f"{key} not in {data}")
    return data[key]


def str_field(key: str, data: dict[str, Any]) -> str:
    return field_of(str, key, data)


def bool_field(key: str, data: dict[str, Any]) -> bool:
    return field_of(bool, key, data)


def float_field(key: str, data: dict[str, Any]) -> float:
    return field_of(float, key, data)


def optional_str_field(key: str, data: dict[str, Any]) -> str | None:
    return optional_field_of(str, key, data)


def optional_int_field(key: str, data: dict[str, Any]) -> int | None:
    return optional_field_of(int, key, data)


def dict_field(key: str, data: dict[str, Any]) -> dict[str, Any]:
    d2 = field_of(dict, key, data)
    return {value_of(str, k): d2[k] for k in d2}


def list_field(t: type[A], key: str, data: dict[str, Any]) -> list[A]:
    items = field_of(list, key, data)
    return [value_of(t, it) for it in items]


def enum_field(key: str, enum: Callable[[A], B], data: dict[str, Any]) -> B:
    return enum(required_field(key, data))
