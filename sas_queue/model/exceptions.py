from typing import Iterable


class BaseError(Exception):
    pass


class BaseWarning(Warning):
    pass


class EventNotHandled(BaseWarning):
    def __init__(self, event: str):
        self.event = event

    def __str__(self) -> str:
        return f"Event type not handled: {self.event}"


class InvalidQueuePublish(BaseError):
    def __init__(self, *, event_name: str, queue_name: str):
        self.event_name = event_name
        self.queue_name = queue_name

    def __str__(self) -> str:
        return (
            f"Event '{self.event_name}' attempted to be published "
            f"to queue '{self.queue_name}', but queue {self.queue_name} is unknown"
        )


class InvalidQueueEventPublish(BaseError):
    def __init__(
        self,
        *,
        event_name: str,
        event_queue_name: str,
        queue_name: str,
    ):
        self.event_name = event_name
        self.event_queue_name = event_queue_name
        self.queue_name = queue_name

    def __str__(self) -> str:
        return (
            f"Event '{self.event_name}' attempted to be published "
            f"to queue '{self.queue_name}', but {self.event_name} events should "
            f"be routed to queue '{self.event_queue_name}'"
        )


class InvalidQueueEventContentPublish(BaseError):
    def __init__(
        self,
        *,
        event_name: str,
        event_content_type: str,
        queue_name: str,
        queue_content_types: Iterable[str],
    ):
        self.event_name = event_name
        self.event_content_type = event_content_type
        self.queue_name = queue_name
        self.queue_content_types = queue_content_types

    def __str__(self) -> str:
        return (
            f"Event '{self.event_name}' attempted to be published "
            f"to queue '{self.queue_name}', but this queue does "
            f"not accept content type '{self.event_content_type}'.\n"
            f"Accepted content types: {', '.join([c for c in self.queue_content_types])}"
        )
