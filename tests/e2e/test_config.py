import json
from pathlib import Path
from unittest import mock

import pytest

from .conftest import CLIRunner


@pytest.mark.smoke
def test_config_save_registry_auth_locally(cli_runner: CLIRunner) -> None:
    result = cli_runner(
        ["apolo-extras", "config", "save-registry-auth", ".docker.json"]
    )
    assert result.returncode == 0, result

    with Path(".docker.json").open() as f:
        payload = json.load(f)

    assert payload == {"auths": mock.ANY}
