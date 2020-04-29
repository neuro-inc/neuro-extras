import base64
import json
import logging
import os
import textwrap
import uuid
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
from typing import Callable, Iterator, List
from unittest import mock

import pytest
import yaml
from _pytest.capture import CaptureFixture
from neuromation.cli.const import EX_OK
from neuromation.cli.main import cli as neuro_main

from neuro_extras.main import main as extras_main


logger = logging.getLogger(__name__)


@pytest.fixture
def project_dir() -> Iterator[Path]:
    with TemporaryDirectory() as cwd_str:
        old_cwd = Path.cwd()
        cwd = Path(cwd_str)
        os.chdir(cwd)
        try:
            yield cwd
        finally:
            os.chdir(old_cwd)


CLIRunner = Callable[[List[str]], CompletedProcess]


@pytest.fixture()
def cli_runner(capfd: CaptureFixture, project_dir: Path) -> CLIRunner:
    def _run_cli(args: List[str]) -> CompletedProcess:  # type: ignore
        cmd = args.pop(0)
        if cmd not in ("neuro", "neuro-extras"):
            pytest.fail(f"Illegal command: {cmd}")

        logger.info(f"Run '{cmd} {' '.join(args)}'",)
        capfd.readouterr()

        main = extras_main
        if cmd == "neuro":
            args = [
                "--show-traceback",
                "--disable-pypi-version-check",
                "--color=no",
            ] + args
            main = neuro_main

        code = EX_OK
        try:
            main(args)
        except SystemExit as e:
            code = e.code
        out, err = capfd.readouterr()
        return CompletedProcess(
            args=[cmd] + args, returncode=code, stdout=out.strip(), stderr=err.strip()
        )

    return _run_cli


def test_init_aliases(cli_runner: CLIRunner) -> None:
    toml_path = Path(".neuro.toml")
    assert not toml_path.exists()

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result
    assert "Added aliases to" in result.stdout, result

    assert toml_path.exists()


def test_image_build_failure(cli_runner: CLIRunner) -> None:
    pkg_path = Path("pkg")
    result = cli_runner(["neuro-extras", "seldon", "init-package", str(pkg_path)])
    result = cli_runner(["neuro-extras", "image", "build", str(pkg_path), "<invalid>"])
    assert result.returncode == 1, result
    assert "repository can only contain" in result.stdout


def test_image_build_custom_dockerfile(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                """\
                FROM ubuntu:latest
                RUN echo !
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"

    result = cli_runner(
        ["neuro", "image-build", "-f", str(dockerfile_path), ".", img_uri_str]
    )
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "image", "tags", "image:extras-e2e"])
    assert result.returncode == 0, result
    assert tag in result.stdout


def test_seldon_deploy_from_local(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    pkg_path = Path("pkg")
    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"
    result = cli_runner(["neuro", "seldon-init-package", str(pkg_path)])
    assert result.returncode == 0, result
    assert "Copying a Seldon package scaffolding" in result.stdout, result

    assert (pkg_path / "Dockerfile").exists()

    result = cli_runner(["neuro", "image-build", str(pkg_path), img_uri_str])
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "image", "tags", "image:extras-e2e"])
    assert result.returncode == 0, result
    assert tag in result.stdout


def test_config_save_docker_json_locally(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "config", "save-docker-json", ".docker.json"])
    assert result.returncode == 0, result

    with Path(".docker.json").open() as f:
        payload = json.load(f)

    assert payload == {"auths": mock.ANY}


def test_k8s_generate_secret(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "k8s", "generate-secret"])
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    assert payload == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "neuro"},
        "type": "Opaque",
        "data": mock.ANY,
    }
    assert payload["data"]["db"]


def test_k8s_generate_secret_custom_name(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "k8s", "generate-secret", "--name", "test"])
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
    result = cli_runner(["neuro-extras", "k8s", "generate-registry-secret"])
    assert result.returncode == 0, result

    payload = yaml.safe_load(result.stdout)
    assert payload == {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "neuro-registry"},
        "type": "kubernetes.io/dockerconfigjson",
        "data": {".dockerconfigjson": mock.ANY},
    }
    docker_config_payload = json.loads(
        base64.b64decode(payload["data"][".dockerconfigjson"])
    )
    assert docker_config_payload == {"auths": mock.ANY}


def test_k8s_generate_registry_secret_custom_name(cli_runner: CLIRunner) -> None:
    result = cli_runner(
        ["neuro-extras", "k8s", "generate-registry-secret", "--name", "test"]
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
                "image": "neuromation/neuro-extras:latest",
                "imagePullPolicy": "Always",
                "command": ["bash", "-c"],
                "args": [
                    "cp -L -r /var/run/neuro/config /root/.neuro;"
                    "chmod 0700 /root/.neuro;"
                    "chmod 0600 /root/.neuro/db;"
                    f"neuro cp storage:model/model.pkl /storage"
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
                "image": "neuromation/neuro-extras:latest",
                "imagePullPolicy": "Always",
                "command": ["bash", "-c"],
                "args": [
                    "cp -L -r /var/run/neuro/config /root/.neuro;"
                    "chmod 0700 /root/.neuro;"
                    "chmod 0600 /root/.neuro/db;"
                    f"neuro cp storage:model/model.pkl /storage"
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
