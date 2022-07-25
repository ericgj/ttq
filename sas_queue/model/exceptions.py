class BaseError(Exception):
    pass


class BaseWarning(Warning):
    pass


class EventNotHandled(BaseWarning):
    def __init__(self, event_type: str):
        self.event_type = event_type

    def __str__(self) -> str:
        return f"Event type not handled: {self.event_type}"
