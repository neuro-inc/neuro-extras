import uuid
from typing import Callable, ContextManager

import pytest

from ..conftest import CLIRunner


@pytest.mark.smoke
@pytest.mark.serial
def test_data_transfer(
    cli_runner: CLIRunner,
    current_user: str,
    switch_cluster: Callable[[str], ContextManager[None]],
    src_cluster: str,
    dst_cluster: str,
) -> None:
    # Note: data-transfer runs copying job on dst_cluster and
    # we pushed test image to src_cluster, so it should be a target cluster
    src_cluster, dst_cluster = dst_cluster, src_cluster

    with switch_cluster(src_cluster):
        result = cli_runner(["apolo-extras", "init-aliases"])
        assert result.returncode == 0, result

        src_path = (
            f"storage://{src_cluster}/{current_user}/copy-src/{str(uuid.uuid4())}"
        )
        result = cli_runner(["apolo", "mkdir", "-p", src_path])
        assert result.returncode == 0, result

        dst_path = (
            f"storage://{dst_cluster}/{current_user}/copy-dst/{str(uuid.uuid4())}"
        )

        result = cli_runner(
            ["apolo", "data-transfer", src_path, dst_path],
        )
        assert result.returncode == 0, result

        del_result = cli_runner(
            ["apolo", "rm", "-r", src_path],
        )
        assert del_result.returncode == 0, result

    with switch_cluster(dst_cluster):
        result = cli_runner(
            ["apolo", "ls", dst_path],
        )
        assert result.returncode == 0, result

        del_result = cli_runner(["apolo", "rm", "-r", dst_path])
        assert del_result.returncode == 0, result
