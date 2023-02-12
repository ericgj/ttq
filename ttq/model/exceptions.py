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
