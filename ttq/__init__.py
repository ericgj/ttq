from .adapter import config_file
from .model.config import Config
from .app import App

__version__ = "0.5"


def run(config: Config, app: App) -> None:
    """
    Primary application interface. Assuming you have ttq stored in a config
    file, call it like this:

        ttq.run( ttq.config(my_config_file), my_app)

    Note that as of v0.5, ttq does _not_ configure logging -- that's left to
    your application.
    """
    from .command.run import run as command_run  # late import to ensure logging set up

    command_run(config, app)


def config(file: str, top_level: str = config_file.TOP_LEVEL) -> Config:
    return config_file.parse_file(file, top_level)
