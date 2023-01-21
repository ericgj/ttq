import logging  # noqa
import sys
from typing import List

from .model.config import Config
from .model import command


def main(argv: List[str] = sys.argv[1:]):
    """
    TODO
    - parse args
    - parse config
    - setup logging from config
    - import app code
    - call exec_<command>
    """
    pass


def exec_run(config: Config, app: command.FromEvent):
    from .command.run import run  # late import to ensure logging set up

    run(config, app)


if __name__ == "__main__":
    main()
