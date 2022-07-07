import sys
import uuid
from pathlib import Path
from unittest import mock

import pytest
import yaml

from neuro_extras.common import NEURO_EXTRAS_IMAGE

from .conftest import CLIRunner


@pytest.mark.smoke
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_seldon_deploy_from_local(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    pkg_path = Path("pkg")
    result = cli_runner(["neuro", "seldon-init-package", str(pkg_path)])
    assert result.returncode == 0, result
    assert "Copying a Seldon package scaffolding" in result.stdout, result

    assert (pkg_path / "seldon.Dockerfile").exists()

    tag = str(uuid.uuid4())
    img_uri = f"image:extras-e2e:{tag}"
    result = cli_runner(
        ["neuro", "image-build", "-f", "seldon.Dockerfile", str(pkg_path), img_uri]
    )
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "image", "size", img_uri])
    assert result.returncode == 0, result

    cli_runner(["neuro", "image", "rm", img_uri])


def test_seldon_generate_deployment(cli_runner: CLIRunner) -> None:
    result = cli_runner(
        [
            "neuro-extras",
            "seldon",
            "generate-deployment",
            "image:model:latest",
            "storage:model/model.pkl",
        ]
    )
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    expected_pod_spec = {
        "volumes": [
            {"emptyDir": {}, "name": "neuro-storage"},
            {"name": "neuro-secret", "secret": {"secretName": "neuro"}},
        ],
        "imagePullSecrets": [{"name": "neuro-registry"}],
        "initContainers": [
            {
                "name": "neuro-download",
                "image": NEURO_EXTRAS_IMAGE,
                "imagePullPolicy": "Always",
                "securityContext": {"runAsUser": 0},
                "command": ["bash", "-c"],
                "args": [
                    "cp -L -r /var/run/neuro/config /root/.neuro;"
                    "chmod 0700 /root/.neuro;"
                    "chmod 0600 /root/.neuro/db;"
                    "neuro cp storage:model/model.pkl /storage"
                ],
                "volumeMounts": [
                    {"mountPath": "/storage", "name": "neuro-storage"},
                    {"mountPath": "/var/run/neuro/config", "name": "neuro-secret"},
                ],
            }
        ],
        "containers": [
            {
                "name": "model",
                "image": mock.ANY,
                "imagePullPolicy": "Always",
                "volumeMounts": [{"mountPath": "/storage", "name": "neuro-storage"}],
            }
        ],
    }
    assert payload == {
        "apiVersion": "machinelearning.seldon.io/v1",
        "kind": "SeldonDeployment",
        "metadata": {"name": "neuro-model"},
        "spec": {
            "predictors": [
                {
                    "componentSpecs": [{"spec": expected_pod_spec}],
                    "graph": {
                        "endpoint": {"type": "REST"},
                        "name": "model",
                        "type": "MODEL",
                    },
                    "name": "predictor",
                    "replicas": 1,
                }
            ]
        },
    }


def test_seldon_generate_deployment_custom(cli_runner: CLIRunner) -> None:
    result = cli_runner(
        [
            "neuro-extras",
            "seldon",
            "generate-deployment",
            "--name",
            "test",
            "--neuro-secret",
            "test-neuro",
            "--registry-secret",
            "test-registry",
            "image:model:latest",
            "storage:model/model.pkl",
        ]
    )
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    expected_pod_spec = {
        "volumes": [
            {"emptyDir": {}, "name": "neuro-storage"},
            {"name": "neuro-secret", "secret": {"secretName": "test-neuro"}},
        ],
        "imagePullSecrets": [{"name": "test-registry"}],
        "initContainers": [
            {
                "name": "neuro-download",
                "image": NEURO_EXTRAS_IMAGE,
                "imagePullPolicy": "Always",
                "securityContext": {"runAsUser": 0},
                "command": ["bash", "-c"],
                "args": [
                    "cp -L -r /var/run/neuro/config /root/.neuro;"
                    "chmod 0700 /root/.neuro;"
                    "chmod 0600 /root/.neuro/db;"
                    "neuro cp storage:model/model.pkl /storage"
                ],
                "volumeMounts": [
                    {"mountPath": "/storage", "name": "neuro-storage"},
                    {"mountPath": "/var/run/neuro/config", "name": "neuro-secret"},
                ],
            }
        ],
        "containers": [
            {
                "name": "model",
                "image": mock.ANY,
                "imagePullPolicy": "Always",
                "volumeMounts": [{"mountPath": "/storage", "name": "neuro-storage"}],
            }
        ],
    }
    assert payload == {
        "apiVersion": "machinelearning.seldon.io/v1",
        "kind": "SeldonDeployment",
        "metadata": {"name": "test"},
        "spec": {
            "predictors": [
                {
                    "componentSpecs": [{"spec": expected_pod_spec}],
                    "graph": {
                        "endpoint": {"type": "REST"},
                        "name": "model",
                        "type": "MODEL",
                    },
                    "name": "predictor",
                    "replicas": 1,
                }
            ]
        },
    }
