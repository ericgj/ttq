from functools import reduce
from time import sleep
from typing import Iterator, Callable, Protocol, Union, List

from ttq.model.event import EventProtocol


class EventStep:
    def __init__(self, event: EventProtocol):
        self.event = event


class WaitStep:
    def __init__(self, secs: float):
        self.secs = secs


class AbortStep:
    def __init__(self, index: int):
        self.index = index


Step = Union[EventStep, AbortStep, WaitStep]


class ScriptHandlerProtocol(Protocol):
    def send(self, event: EventProtocol) -> str: ...

    def abort(self, routing_key: str) -> None: ...

    def finish(self, keys: List[str]) -> None: ...


class Script:
    def __init__(self, steps: List[Step] = []):
        self._steps = steps

    def and_then(self, other: "Script") -> "Script":
        return Script(self._steps + other._steps)

    def and_send(self, event: EventProtocol) -> "Script":
        return Script(self._steps + [EventStep(event)])

    def and_send_repeatedly(
        self,
        event_func: Callable[[int], EventProtocol],
        times: int,
        every: float = 1.0,
    ) -> "Script":
        def _accum(script: "Script", i: int) -> "Script":
            if i == 0:
                return script.and_send(event_func(i))
            else:
                return script.and_wait(every).and_send(event_func(i))

        return reduce(_accum, range(times), self)

    def and_wait(self, secs: float) -> "Script":
        return Script(self._steps + [WaitStep(secs)])

    def and_abort(self, index: int) -> "Script":
        return Script(self._steps + [AbortStep(index)])

    def __iter__(self) -> Iterator[Step]:
        return iter(self._steps)

    def run(self, handler: ScriptHandlerProtocol) -> None:
        results: List[str] = []
        for step in self:
            if isinstance(step, WaitStep):
                sleep(step.secs)

            elif isinstance(step, AbortStep):
                handler.abort(results[step.index])

            elif isinstance(step, EventStep):
                results.append(handler.send(step.event))

            else:
                raise ValueError(f"Unknown step type {type(step)}")

        handler.finish(results)


def send(event: EventProtocol) -> Script:
    return Script([EventStep(event)])


def send_repeatedly(
    event_func: Callable[[int], EventProtocol],
    times: int,
    every: float = 1.0,
) -> Script:
    def _accum(steps: List[Step], i: int) -> List[Step]:
        event_step = EventStep(event_func(i))
        if i > 0:
            steps.append(WaitStep(every))
        steps.append(event_step)
        return steps

    empty_steps: List[Step] = []
    return Script(reduce(_accum, range(times), empty_steps))


def wait(secs: float) -> Script:
    return Script([WaitStep(secs)])
