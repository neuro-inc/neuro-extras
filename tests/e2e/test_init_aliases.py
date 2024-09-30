import logging
from pathlib import Path

import pytest

from .conftest import CLIRunner


root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


@pytest.mark.smoke
def test_init_aliases(cli_runner: CLIRunner) -> None:
    toml_path = Path(".neuro.toml")
    assert not toml_path.exists()

    result = cli_runner(["apolo-extras", "init-aliases"])
    assert result.returncode == 0, result
    assert "Added aliases to" in result.stdout, result

    assert toml_path.exists()
