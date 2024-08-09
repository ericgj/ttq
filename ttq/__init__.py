__version__ = "0.4"

import logging.config

from .adapter import config_file
from .model.config import Config
from .model.command import EventMapping, Command  # noqa


def run(config: Config, app: EventMapping):
    from .command.run import run as command_run  # late import to ensure logging set up

    command_run(config, app)


def run_using_config_file(file: str, app: EventMapping):
    config, logging_config = config_file.parse_file(file)
    if logging_config is not None:
        logging.config.dictConfig(logging_config)
    run(config, app)
