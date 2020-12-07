import logging

import click

from .version import __version__


@click.group()
@click.version_option(
    version=__version__, message="neuro-extras package version: %(version)s"
)
def main() -> None:
    """
    Auxiliary scripts and recipes for automating routine tasks.
    """
    handler = ClickLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


class ClickLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            click.echo(msg)
        except Exception:
            self.handleError(record)
