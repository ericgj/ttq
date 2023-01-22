from argparse import ArgumentParser
import logging
import os.path
import sys
from typing import List, Mapping, cast

from .adapter import config_file
from .model.config import Config
from .model import command
from .util.importlib import import_from_module


def main(argv: List[str] = sys.argv[1:]):
    name = os.path.basename(sys.argv[0])
    cmd = ArgumentParser(prog=name, description="Typed task queue using RabbitMQ")
    cmd.add_argument(
        "-c",
        "--config",
        default=f"{name}.toml",
        help=f"Config file (default {name}.toml)",
    )
    cmd.add_argument("app", help="Event to command mapping (module.variable)")

    args = cmd.parse_args(argv)

    if not os.path.exists(args.config):
        raise ValueError(f"The config file you specified does not exist: {args.config}")

    config, logging_config = config_file.parse_file(args.config)
    if logging_config is not None:
        logging.config.dictConfig(logging_config)

    app = import_app(args.app)

    exec_run(config, app)


def import_app(name) -> command.EventMapping:
    map = import_from_module(name)

    if not isinstance(map, Mapping):
        raise ValueError(f"The variable you specified is not a mapping: {name}")

    cast(command.EventMapping, map)
    return map


def exec_run(config: Config, app: command.EventMapping):
    from .command.run import run  # late import to ensure logging set up

    run(config, app)


if __name__ == "__main__":
    main()
