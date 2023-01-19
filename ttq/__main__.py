import logging  # noqa
import sys
from typing import List

from .model.config import Config


def main(argv: List[str] = sys.argv[1:]):
    """
    TODO
    - parse args
    - parse config
    - setup logging
    - call exec_<command>
    """
    pass


def exec_run(config: Config):
    from .command.run import run  # late import to ensure logging set up

    run(config)


if __name__ == "__main__":
    main()
