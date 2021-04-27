import logging
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import Callable, ContextManager, Iterator, List, Optional, Union

import neuro_sdk as neuro_api  # NOTE: don't use async test functions (issue #129)
import pytest
from neuro_cli.asyncio_utils import run as run_async, setup_child_watcher

from neuro_extras.common import NEURO_EXTRAS_IMAGE


CLIRunner = Callable[[List[str]], CompletedProcess]

logger = logging.getLogger(__name__)

TESTED_ARCHIVE_TYPES = ["tar.gz", "tgz", "zip", "tar"]

setup_child_watcher()


@dataclass
class Secret:
    name: str
    value: str


def generate_random_secret(name_prefix: str = "secret") -> Secret:
    return Secret(
        name=f"{name_prefix}-{uuid.uuid4().hex[:8]}",
        value=str(uuid.uuid4()),
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


@pytest.fixture(scope="session", autouse=True)
def print_neuro_extras_image() -> None:
    logger.warning(f"Using neuro-extras image: '{NEURO_EXTRAS_IMAGE}'")


async def _async_get_bare_client() -> neuro_api.Client:
    """Return uninitialized neuro client."""
    return await neuro_api.get()


@pytest.fixture
def _neuro_client() -> Iterator[neuro_api.Client]:
    # Note: because of issue #129 we can't use async methods of the client,
    # therefore this fixture is private
    client = run_async(_async_get_bare_client())
    try:
        yield run_async(client.__aenter__())
    finally:
        run_async(client.__aexit__())  # it doesn't use arguments


@pytest.fixture
def current_cluster(_neuro_client: neuro_api.Client) -> str:
    return _neuro_client.cluster_name


@pytest.fixture
def current_user(_neuro_client: neuro_api.Client) -> str:
    return _neuro_client.username


@pytest.fixture
def switch_cluster(
    _neuro_client: neuro_api.Client,
) -> Callable[[str], ContextManager[None]]:
    @contextmanager
    def _f(cluster: str) -> Iterator[None]:
        orig_cluster = _neuro_client.config.cluster_name
        try:
            logger.info(f"Temporary cluster switch: {orig_cluster} -> {cluster}")
            run_async(_neuro_client.config.switch_cluster(cluster))
            yield
        finally:
            logger.info(f"Switch back cluster: {cluster} -> {orig_cluster}")
            try:
                run_async(_neuro_client.config.switch_cluster(orig_cluster))
            except Exception as e:
                logger.error(
                    f"Could not switch back to cluster '{orig_cluster}': {e}. "
                    f"Please run manually: 'neuro config switch-cluster {orig_cluster}'"
                )

    return _f
