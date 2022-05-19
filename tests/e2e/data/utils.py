import logging
import os
from typing import Any, Iterator, List, Tuple

from ..conftest import run_cli


logger = logging.getLogger(__name__)


def get_tested_archive_types() -> List[str]:
    env = os.environ.get("PYTEST_DATA_COPY_ARCHIVE_TYPES")
    if env:
        return env.split(",")
    else:
        return [".tar.gz"]


def _run_command(cmd: str, args: List[str]) -> Tuple[int, str, str]:
    logger.info(f"Running: {[cmd] + args}")
    result = run_cli([cmd] + args)
    return result.returncode, result.stdout, result.stderr


def _resume_fixture_iterator(iterator: Iterator[Any]) -> None:
    try:
        next(iterator)
    except StopIteration:
        pass
