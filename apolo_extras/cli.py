import logging

import click

from .version import __version__


class ClickLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            click.echo(msg)
        except Exception:
            self.handleError(record)


@click.group()
@click.option(
    "-v",
    "--verbose",
    count=True,
    type=int,
    default=0,
    help="Give more output. Option is additive, and can be used up to 2 times.",
)
@click.option(
    "-q",
    "--quiet",
    count=True,
    type=int,
    default=0,
    help="Give less output. Option is additive, and can be used up to 2 times.",
)
@click.version_option(
    version=__version__, message="apolo-extras package version: %(version)s"
)
def main(
    verbose: int,
    quiet: int,
) -> None:
    """
    Auxiliary scripts and recipes for automating routine tasks.
    """
    handler = ClickLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root_logger = logging.getLogger()
    verbosity = verbose - quiet
    if verbosity < -1:
        loglevel = logging.CRITICAL
    elif verbosity == -1:
        loglevel = logging.WARNING
    elif verbosity == 0:
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG
    handler.setLevel(loglevel)

    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
