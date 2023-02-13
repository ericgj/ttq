from functools import reduce
import logging
import re
from typing import List, Optional, Tuple


def assert_log_matching(
    expr: str, records: List[logging.LogRecord], level: Optional[int] = None
):
    actual = first_log_matching(expr, records, level)
    assert actual is not None, "".join(
        [f"Expected at least one log matching /{expr}/ "] + []
        if level is None
        else [f"with level = {logging.getLevelName(level)}"]
    )


def assert_no_log_matching(
    expr: str, records: List[logging.LogRecord], level: Optional[int] = None
):
    actual = logs_matching(expr, records, level)
    assert len(actual) == 0, "".join(
        [f"Expected no logs matching /{expr}/ "] + []
        if level is None
        else [f"with level = {logging.getLevelName(level)} "]
        + [f"but found {len(actual)}"]
    )


def assert_logs_matching(
    expr_levels: List[Tuple[str, Optional[int]]], records: List[logging.LogRecord]
):
    actuals = [
        (expr, level, logs_matching(expr, records, level))
        for (expr, level) in expr_levels
    ]
    errs = [
        "".join(
            [f"Expected at least one log matching /{expr}/ "] + []
            if level is None
            else [f"with level = {logging.getLevelName(level)}"]
        )
        for (expr, level, logs) in actuals
        if len(logs) == 0
    ]
    assert len(errs) == 0, "\n".join(errs)


def assert_logs_matching_in_order(
    expr_levels: List[Tuple[str, Optional[int]]], records: List[logging.LogRecord]
):
    def _is_ordered(
        last: Tuple[int, bool],
        actual: Tuple[str, Optional[int], List[Tuple[int, logging.LogRecord]]],
    ) -> Tuple[int, bool]:
        last_max, value = last
        expr, level, logs = actual
        index = None if len(logs) == 0 else logs[0][0]  # index of first instance
        if not value or index is None:
            return (last_max, value)
        else:
            return (max(index, last_max), value and index > last_max)

    actuals = [
        (expr, level, logs_matching(expr, records, level))
        for (expr, level) in expr_levels
    ]
    errs = [
        "".join(
            [f"Expected at least one log matching /{expr}/ "] + []
            if level is None
            else [f"with level = {logging.getLevelName(level)}"]
        )
        for (expr, level, logs) in actuals
        if len(logs) == 0
    ]
    assert len(errs) == 0, "\n".join(errs)
    assert reduce(_is_ordered, actuals, (-1, True))[1], "\n".join(
        ["Logs matched but were out of order: "]
        + [
            f"/{expr}/: " + ", ".join([str(i) for (i, _) in logs])
            for (expr, level, logs) in actuals
        ]
    )


def first_log_matching(
    expr: str, records: List[logging.LogRecord], level: Optional[int] = None
) -> Optional[Tuple[int, logging.LogRecord]]:
    matches = log_matches(expr, level)
    try:
        return next((i, r) for (i, r) in enumerate(records) if matches(r))
    except StopIteration:
        return None


def logs_matching(
    expr: str, records: List[logging.LogRecord], level: Optional[int] = None
) -> List[Tuple[int, logging.LogRecord]]:
    matches = log_matches(expr, level)
    return [(i, r) for (i, r) in enumerate(records) if matches(r)]


def log_matches(expr: str, level: Optional[int] = None):
    def _matches(record) -> bool:
        m = re.search(expr, record.message)
        return (level is None or level == record.levelno) and (m is not None)

    return _matches
