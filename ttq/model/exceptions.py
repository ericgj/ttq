from pika.spec import BasicProperties


class BaseError(Exception):
    pass


class BaseWarning(Warning):
    pass


class MessageMissingRequiredProperty(BaseError):
    def __init__(self, prop: str, properties: BasicProperties):
        self.prop = prop
        self.properties = properties

    def __str__(self) -> str:
        return (
            "Message missing required property and could not be processed: "
            f"{self.prop}. properties = {self.properties}"
        )


class RedeliverLimit(BaseError):
    def __init__(self, correlation_id: str, *, header: str, count: int, limit: int):
        self.correlation_id = correlation_id
        self.header = header
        self.count = count
        self.limit = limit

    def __str__(self) -> str:
        return (
            f"Cannot redeliver message correlation_id = {self.correlation_id}: "
            f"redelivery limit = {self.limit} reached "
            f"(using header '{self.header}')"
        )


class RedeliverOriginalExchangeMissing(BaseError):
    def __init__(self, correlation_id: str, *, header: str):
        self.correlation_id = correlation_id
        self.header = header

    def __str__(self) -> str:
        return (
            f"Cannot redeliver message correlation_id = {self.correlation_id}: "
            "missing original exchange name "
            f"(using header '{self.header}')"
        )


class EventNotHandled(BaseError):
    def __init__(self, type_name: str, content_type: str):
        self.type_name = type_name
        self.content_type = content_type

    def __str__(self) -> str:
        return f"Event type not handled: {self.type_name} with content type: {self.content_type}"


class NoEncodingForContentType(BaseError):
    def __init__(self, type_name: str, content_type: str):
        self.type_name = type_name
        self.content_type = content_type

    def __str__(self) -> str:
        return (
            f"No encoding for: {self.type_name} with content type: {self.content_type}"
        )


class ProcessNotFoundWarning(BaseWarning):
    def __init__(self, correlation_id: str):
        self.correlation_id = correlation_id

    def __str__(self) -> str:
        return f"No process found for correlation_id {self.correlation_id}"
