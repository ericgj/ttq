from concurrent.futures import Future
from logging import Logger
from typing import Any, Type, Optional, Union, Dict


def log_exceptions(
    logger: Logger,
    *,
    exception_class: Type[Exception] = Exception,
    message: Optional[str] = None,
    context: Dict[str, Any] = {},
    timeout: Union[None, float, int] = None,
):
    def _log_exceptions(f: Future):
        try:
            f.result(timeout=timeout)
        except exception_class as e:
            if message is not None:
                logger.warning(message.format(**context), context)
            logger.exception(e)

    return _log_exceptions
