from typing import TypeVar, Generic, Callable, Iterable, Optional, Tuple, List


R = TypeVar("R")


class Expect(Generic[R]):
    def __init__(self, exp: Callable[[R], bool]):
        self._exp = exp

    def __and__(self, other: "Expect[R]") -> "Expect[R]":
        exp: Expect[R] = Expect(lambda r: self._exp(r) and other._exp(r))
        exp._exp.__name__ = f"({exp_name(self._exp)} & {exp_name(other._exp)})"
        return exp

    def __or__(self, other: "Expect[R]") -> "Expect[R]":
        exp: Expect[R] = Expect(lambda r: self._exp(r) or other._exp(r))
        exp._exp.__name__ = f"({exp_name(self._exp)} | {exp_name(other._exp)})"
        return exp

    def __invert__(self) -> "Expect[R]":
        exp: Expect[R] = Expect(lambda r: not self._exp(r))
        exp._exp.__name__ = f"~({exp_name(self._exp)})"
        return exp

    def __str__(self) -> str:
        return exp_name(self._exp)

    def __call__(self, r: R) -> bool:
        return self._exp(r)


def that(exp: Callable[[R], bool]) -> Expect[R]:
    return Expect(exp)


def exp_name(x) -> str:
    if getattr(x, "__name__"):
        return str(x.__name__)
    else:
        return str(x)


class EvaluationError(Exception, Generic[R]):
    def __init__(
        self,
        actuals: List[R],
        faileds: List[Tuple[int, Expect[R], R]],
        unexpecteds: List[Tuple[int, R]],
        missings: List[Tuple[int, Expect[R]]],
    ):
        self.actuals = actuals
        self.faileds = faileds
        self.unexpecteds = unexpecteds
        self.missings = missings

    @property
    def summary(self) -> str:
        if (
            len(self.faileds) == 0
            and len(self.unexpecteds) == 0
            and len(self.missings) == 0
        ):
            return "EvaluationError: no errors"  # Note: should not reach this
        return (
            f"EvaluationError: {len(self.faileds)} results failed expectations, "
            f"{len(self.unexpecteds)} results were unexpected, "
            f"and {len(self.missings)} expectations were not met with any results. "
        )

    @property
    def faileds_lines(self) -> List[str]:
        lines: List[str] = []
        if len(self.faileds) == 0:
            return lines
        return ["", "FAILURES:", "----------"] + [
            f"expected #{i+1}: {e}\nactual: {a}" for (i, e, a) in self.faileds
        ]

    @property
    def unexpecteds_lines(self) -> List[str]:
        lines: List[str] = []
        if len(self.unexpecteds) == 0:
            return lines
        return ["", "UNEXPECTED:", "-----------"] + [
            f"actual #{i+1}: {a}" for (i, a) in self.unexpecteds
        ]

    @property
    def missings_lines(self) -> List[str]:
        lines: List[str] = []
        if len(self.missings) == 0:
            return lines
        return ["", "MISSING:", "--------"] + [
            f"expected #{i+1}: {e}" for (i, e) in self.missings
        ]

    @property
    def actuals_lines(self) -> List[str]:
        lines: List[str] = []
        if len(self.actuals) == 0:
            return lines
        return ["", "ALL RESULTS RECEIVED:", "--------------------"] + [
            f"actual #{i+1}: {a}" for (i, a) in enumerate(self.actuals)
        ]

    def __str__(self) -> str:

        return "\n".join(
            [self.summary]
            + self.faileds_lines
            + self.unexpecteds_lines
            + self.missings_lines
            + self.actuals_lines
        )


class Evaluation(Generic[R]):
    @classmethod
    def empty(cls) -> "Evaluation[R]":
        return cls([])

    @classmethod
    def unexpected(cls, actual_pair: Tuple[int, R]) -> "Evaluation[R]":
        return cls(unexpecteds=[actual_pair])

    @classmethod
    def missing(cls, expected_pair: Tuple[int, Expect[R]]) -> "Evaluation[R]":
        return cls(missings=[expected_pair])

    def __init__(
        self,
        evals: List[Tuple[int, Expect[R], R]] = [],
        *,
        unexpecteds: List[Tuple[int, R]] = [],
        missings: List[Tuple[int, Expect[R]]] = [],
    ):
        self._evals = evals
        self._unexpecteds = unexpecteds
        self._missings = missings

    def __and__(self, other: "Evaluation[R]") -> "Evaluation[R]":
        return Evaluation(
            self._evals + other._evals,
            unexpecteds=self._unexpecteds + other._unexpecteds,
            missings=self._missings + other._missings,
        )

    @property
    def result(self) -> Optional[EvaluationError]:
        failed = [(i, e, a) for (i, e, a) in self._evals if not e(a)]
        if (
            len(failed) == 0
            and len(self._unexpecteds) == 0
            and len(self._missings) == 0
        ):
            return None
        actuals = [a for (i, e, a) in self._evals] + [a for (i, a) in self._unexpecteds]
        return EvaluationError(
            actuals, failed, unexpecteds=self._unexpecteds, missings=self._missings
        )


def build_evaluation(
    expects: Iterable[Expect[R]], actuals: Iterable[R]
) -> Evaluation[R]:
    actuals_i = enumerate(actuals)
    expects_i = enumerate(expects)
    eval: Evaluation[R] = Evaluation.empty()
    more_actuals = True
    more_expects = True
    while more_actuals or more_expects:
        try:
            actual_i, actual = next(actuals_i)
        except StopIteration:
            more_actuals = False

        try:
            expect_i, expect = next(expects_i)
        except StopIteration:
            more_expects = False

        if more_actuals and more_expects:
            eval = eval & Evaluation([(expect_i, expect, actual)])
        elif not more_actuals and more_expects:
            eval = eval & Evaluation.missing((expect_i, expect))
        elif more_actuals and not more_expects:
            eval = eval & Evaluation.unexpected((actual_i, actual))

    return eval


def evaluate(expects: Iterable[Expect[R]], actuals: Iterable[R]):
    e = build_evaluation(expects, actuals)
    r = e.result
    if r is not None:
        raise r
