from time import sleep
from typing import Iterator, List, Protocol, Union

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
    def send(self, event: EventProtocol) -> str:
        ...

    def abort(self, routing_key: str):
        ...


class Script:
    def __init__(self, steps: List[Step] = []):
        self._steps = steps

    def and_then(self, other: "Script") -> "Script":
        return Script(self._steps + other._steps)

    def and_send(self, event: EventProtocol) -> "Script":
        return Script(self._steps + [EventStep(event)])

    def and_wait(self, secs: float) -> "Script":
        return Script(self._steps + [WaitStep(secs)])

    def and_abort(self, index: int) -> "Script":
        return Script(self._steps + [AbortStep(index)])

    def __iter__(self) -> Iterator[Step]:
        return iter(self._steps)

    def run(self, handler: ScriptHandlerProtocol):
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


def send(event: EventProtocol) -> Script:
    return Script([EventStep(event)])
