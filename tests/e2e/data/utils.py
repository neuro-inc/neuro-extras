import logging
from typing import List, Tuple

from ..conftest import run_cli


logger = logging.getLogger(__name__)


def _run_command(cmd: str, args: List[str]) -> Tuple[int, str, str]:
    logger.info(f"Running: {[cmd] + args}")
    result = run_cli([cmd] + args)
    return result.returncode, result.stdout, result.stderr
