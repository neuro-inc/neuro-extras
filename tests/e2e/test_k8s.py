import base64
import json
from unittest import mock

import pytest
import yaml

from .conftest import CLIRunner


@pytest.mark.smoke
def test_k8s_generate_secret(cli_runner: CLIRunner) -> None:
    result = cli_runner(["apolo-extras", "k8s", "generate-secret"])
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    assert payload == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "apolo"},
        "type": "Opaque",
        "data": mock.ANY,
    }
    assert payload["data"]["db"]


def test_k8s_generate_secret_custom_name(cli_runner: CLIRunner) -> None:
    result = cli_runner(["apolo-extras", "k8s", "generate-secret", "--name", "test"])
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    assert payload == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test"},
        "type": "Opaque",
        "data": mock.ANY,
    }
    assert payload["data"]["db"]


def test_k8s_generate_registry_secret(cli_runner: CLIRunner) -> None:
    result = cli_runner(["apolo-extras", "k8s", "generate-registry-secret"])
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    assert payload == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "apolo-registry"},
        "type": "kubernetes.io/dockerconfigjson",
        "data": {".dockerconfigjson": mock.ANY},
    }
    docker_config_payload = json.loads(
        base64.b64decode(payload["data"][".dockerconfigjson"])
    )
    assert docker_config_payload == {"auths": mock.ANY}


def test_k8s_generate_registry_secret_custom_name(cli_runner: CLIRunner) -> None:
    result = cli_runner(
        ["apolo-extras", "k8s", "generate-registry-secret", "--name", "test"]
    )
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    assert payload == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test"},
        "type": "kubernetes.io/dockerconfigjson",
        "data": {".dockerconfigjson": mock.ANY},
    }
