import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import Callable, Iterator, List, Optional, Tuple, Union

import pytest


CLIRunner = Callable[[List[str]], CompletedProcess]

logger = logging.getLogger(__name__)


@dataclass
class Secret:
    name: str
    value: str


def generate_random_secret(name_prefix: str = "secret") -> Secret:
    return Secret(
        name=f"{name_prefix}-{uuid.uuid4().hex[:8]}", value=str(uuid.uuid4()),
    )


@pytest.fixture
def temp_random_secret(cli_runner: CLIRunner) -> Iterator[Secret]:
    secret = generate_random_secret()
    try:
        yield secret
    finally:
        r = cli_runner(["neuro", "secret", "rm", secret.name])
        if r.returncode != 0:
            details = f"code {r.returncode}, stdout: `{r.stdout}`, stderr: `{r.stderr}`"
            logger.warning(f"Could not delete secret '{secret.name}', {details}")


def gen_random_file(location: Union[str, Path], name: Optional[str] = None) -> Path:
    location = Path(location)
    location.mkdir(parents=True, exist_ok=True)
    name = name or f"file-{uuid.uuid4().hex[:8]}.txt"
    file = location / name
    file.write_text(str(uuid.uuid4()))
    return file


def gen_ing_extr_strategies_grid() -> List[Tuple[str, str, str, bool]]:
    grid = []
    for src_type in ["aws"]:
        for dst_type in ["storage:"]:
            for archive_extension in ["tar.gz"]:
                for extract in [False]:
                    grid.append((src_type, dst_type, archive_extension, extract))
    return grid
