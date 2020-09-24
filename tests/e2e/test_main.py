import base64
import json
import logging
import os
import re
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Callable, Iterator, List
from unittest import mock

import pytest
import toml
import yaml
from _pytest.capture import CaptureFixture
from neuromation.cli.const import EX_OK
from neuromation.cli.main import cli as neuro_main

from neuro_extras.main import TEMP_UNPACK_DIR, main as extras_main

from .conftest import CLIRunner, Secret, gen_random_file


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


GCP_BUCKET = "gs://cookiecutter-e2e"
AWS_BUCKET = "s3://cookiecutter-e2e"


@pytest.fixture()
def cli_runner(capfd: CaptureFixture, project_dir: Path) -> CLIRunner:
    def _run_cli(args: List[str]) -> subprocess.CompletedProcess:  # type: ignore
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
        return subprocess.CompletedProcess(
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
    result = cli_runner(
        [
            "neuro-extras",
            "image",
            "build",
            "-f",
            "seldon.Dockerfile",
            str(pkg_path),
            "<invalid>",
        ]
    )
    assert result.returncode == 1, result
    assert "repository can only contain" in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_custom_dockerfile(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    tag = str(uuid.uuid4())

    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    img_name = f"image:extras-e2e-custom-dockerfile-{rnd}"
    img_uri_str = f"{img_name}:{tag}"

    result = cli_runner(
        ["neuro", "image-build", "-f", str(dockerfile_path), ".", img_uri_str]
    )
    assert result.returncode == 0, result
    sleep(10)

    result = cli_runner(["neuro", "image", "tags", img_name])
    assert result.returncode == 0, result
    assert tag in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_ignored_files_are_not_copied(cli_runner: CLIRunner,) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    ignored_file = "this_file_should_not_be_added.txt"
    ignored_file_content = "this should not be printed\n"

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    Path(".neuroignore").write_text(f"{ignored_file}\n")
    Path(ignored_file).write_text(ignored_file_content)
    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    Path(dockerfile_path).write_text(
        textwrap.dedent(
            f"""\
            FROM ubuntu:latest
            ADD {random_file_to_disable_layer_caching} /tmp
            ADD {ignored_file} /
            RUN cat /{ignored_file}
            """
        )
    )

    img_uri_str = f"image:extras-e2e:{uuid.uuid4()}"

    result = cli_runner(
        ["neuro", "image-build", "-f", str(dockerfile_path), ".", img_uri_str]
    )

    assert ignored_file_content not in result.stdout


def test_storage_copy(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "config", "show"])
    username_re = re.compile(".*User Name: ([a-zA-Z0-9-]+).*", re.DOTALL)
    cluster_re = re.compile(".*Current Cluster: ([a-zA-Z0-9-]+).*", re.DOTALL)
    m = username_re.match(result.stdout)
    assert m
    username = m.groups()[0]
    m = cluster_re.match(result.stdout)
    assert m
    current_cluster = m.groups()[0]

    run_id = uuid.uuid4()
    src_path = f"copy-src/{str(run_id)}"
    result = cli_runner(["neuro", "mkdir", "-p", "storage:" + src_path])
    assert result.returncode == 0, result

    dst_path = "copy-dst"

    result = cli_runner(
        [
            "neuro",
            "storage-cp",
            f"storage://{current_cluster}/{username}/{src_path}",
            f"storage://{current_cluster}/{username}/{dst_path}",
        ]
    )
    assert result.returncode == 0, result


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_copy(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    image = f"image:extras-e2e-image-copy-{rnd}"

    tag = str(uuid.uuid4())
    img_uri_str = f"{image}:{tag}"

    result = cli_runner(
        ["neuro", "image-build", "-f", str(dockerfile_path), ".", img_uri_str]
    )
    assert result.returncode == 0, result
    sleep(10)

    result = cli_runner(["neuro", "image", "tags", image])
    assert result.returncode == 0, result
    assert tag in result.stdout

    result = cli_runner(["neuro", "image-copy", img_uri_str, image])
    assert result.returncode == 0, result
    sleep(10)
    result = cli_runner(["neuro", "image", "tags", image])
    assert result.returncode == 0, result


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_custom_build_args(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                ARG TEST_ARG
                ARG ANOTHER_TEST_ARG
                RUN echo $TEST_ARG
                RUN echo $ANOTHER_TEST_ARG
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"

    result = cli_runner(
        [
            "neuro",
            "image-build",
            "-f",
            str(dockerfile_path),
            "--build-arg",
            f"TEST_ARG=arg-{tag}",
            "--build-arg",
            f"ANOTHER_TEST_ARG=arg-another-{tag}",
            ".",
            img_uri_str,
        ]
    )
    assert result.returncode == 0, result
    assert f"arg-{tag}" in result.stdout
    assert f"arg-another-{tag}" in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_env(cli_runner: CLIRunner, temp_random_secret: Secret) -> None:
    sec = temp_random_secret

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "secret", "add", sec.name, sec.value])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                ARG GIT_TOKEN
                ENV GIT_TOKEN=$GIT_TOKEN
                RUN echo git_token=$GIT_TOKEN
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"

    result = cli_runner(
        [
            "neuro",
            "image-build",
            "-f",
            str(dockerfile_path),
            "-e",
            f"GIT_TOKEN=secret:{sec.name}",
            ".",
            img_uri_str,
        ]
    )
    assert result.returncode == 0, result
    assert f"git_token={sec.value}" in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_volume(cli_runner: CLIRunner, temp_random_secret: Secret) -> None:
    sec = temp_random_secret

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "secret", "add", sec.name, sec.value])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                ADD secret.txt /
                RUN echo git_token=$(cat secret.txt)
                """
            )
        )

    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    image = f"image:extras-e2e-image-copy-{rnd}"

    tag = str(uuid.uuid4())
    img_uri_str = f"{image}:{tag}"

    result = cli_runner(
        [
            "neuro",
            "image-build",
            "-f",
            str(dockerfile_path),
            "-v",
            f"secret:{sec.name}:/workspace/secret.txt",
            ".",
            img_uri_str,
        ]
    )
    assert result.returncode == 0, result
    assert f"git_token={sec.value}" in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_seldon_deploy_from_local(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    pkg_path = Path("pkg")
    tag = str(uuid.uuid4())
    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    img_name = f"image:extras-e2e-seldon-local-{rnd}"
    img_uri_str = f"{img_name}:{tag}"
    result = cli_runner(["neuro", "seldon-init-package", str(pkg_path)])
    assert result.returncode == 0, result
    assert "Copying a Seldon package scaffolding" in result.stdout, result

    assert (pkg_path / "seldon.Dockerfile").exists()

    result = cli_runner(
        ["neuro", "image-build", "-f", "seldon.Dockerfile", str(pkg_path), img_uri_str]
    )
    assert result.returncode == 0, result
    sleep(10)

    result = cli_runner(["neuro", "image", "tags", img_name])
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
                "image": "neuromation/neuro-extras:latest",
                "imagePullPolicy": "Always",
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


@pytest.fixture()
def remote_project_dir(project_dir: Path) -> Path:
    local_conf = project_dir / ".neuro.toml"
    remote_project_dir = "e2e-test-remote-dir"
    local_conf.write_text(
        toml.dumps({"extra": {"remote-project-dir": remote_project_dir}})
    )
    return Path(remote_project_dir)


def test_upload_download_single_file(
    project_dir: Path, remote_project_dir: Path, cli_runner: CLIRunner
) -> None:
    test_file_name = "test.txt"
    test_file_content = "Testing"

    file = project_dir / test_file_name
    file.write_text(test_file_content)
    result = cli_runner(["neuro-extras", "upload", test_file_name])
    assert result.returncode == 0, result
    file.unlink()
    # Redownload file
    result = cli_runner(["neuro-extras", "download", test_file_name])
    assert result.returncode == 0, result
    file = project_dir / test_file_name
    assert file.read_text() == test_file_content


def test_upload_download_subdir(
    project_dir: Path, remote_project_dir: Path, cli_runner: CLIRunner
) -> None:
    subdir_name = "sub"
    test_file_name = "test.txt"
    test_file_content = "Testing"

    file_in_root = project_dir / test_file_name
    file_in_root.write_text(test_file_content)
    subdir = project_dir / subdir_name
    subdir.mkdir()
    file_in_subdir = project_dir / subdir_name / test_file_name

    file_in_subdir.write_text(test_file_content)

    result = cli_runner(["neuro-extras", "upload", subdir_name])
    assert result.returncode == 0, result
    file_in_root.unlink()
    file_in_subdir.unlink()
    subdir.rmdir()
    # Redownload folder
    result = cli_runner(["neuro-extras", "download", subdir_name])
    assert result.returncode == 0, result
    file_in_root = project_dir / test_file_name
    assert not file_in_root.exists(), "File in project root should not be downloaded"
    file_in_subdir = project_dir / subdir_name / test_file_name
    assert file_in_subdir.read_text() == test_file_content


@pytest.fixture
def args_data_cp_from_cloud(cli_runner: CLIRunner) -> Callable[..., List[str]]:
    def _f(bucket: str, src: str, dst: str, extract: bool) -> List[str]:
        args = ["neuro-extras", "data", "cp", src, dst]
        if src.startswith("storage:") or dst.startswith("storage:"):
            if bucket.startswith("gs://"):
                args.extend(
                    [
                        "-v",
                        "secret:neuro-extras-gcp:/gcp-creds.txt",
                        "-e",
                        "GOOGLE_APPLICATION_CREDENTIALS=/gcp-creds.txt",
                    ]
                )
            elif bucket.startswith("s3://"):
                args.extend(
                    [
                        "-v",
                        "secret:neuro-extras-aws:/aws-creds.txt",
                        "-e",
                        "AWS_CONFIG_FILE=/aws-creds.txt",
                    ]
                )
            else:
                raise NotImplementedError(bucket)
        if extract:
            args.append("-x")
        return args

    return _f


@pytest.mark.parametrize("bucket", [GCP_BUCKET, AWS_BUCKET])
@pytest.mark.parametrize("archive_extension", ["tar.gz", "tgz", "zip", "tar"])
@pytest.mark.parametrize("extract", [True, False])
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows path are not supported yet + no utilities on windows",
)
def test_data_cp_from_cloud_to_local(
    project_dir: Path,
    remote_project_dir: Path,
    cli_runner: CLIRunner,
    args_data_cp_from_cloud: Callable[..., List[str]],
    bucket: str,
    archive_extension: str,
    extract: bool,
) -> None:
    TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=TEMP_UNPACK_DIR.expanduser()) as tmp_dir:
        src = f"{bucket}/hello.{archive_extension}"
        res = cli_runner(args_data_cp_from_cloud(bucket, src, tmp_dir, extract))
        assert res.returncode == 0, res

        if extract:
            expected_file = Path(tmp_dir) / "data" / "hello.txt"
            assert "Hello world!" in expected_file.read_text()
        else:
            expected_archive = Path(tmp_dir) / f"hello.{archive_extension}"
            assert expected_archive.is_file()


@pytest.mark.parametrize("bucket", [GCP_BUCKET, AWS_BUCKET])
@pytest.mark.parametrize("archive_extension", ["tar.gz"])
@pytest.mark.parametrize("extract", [True, False])
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows path are not supported yet + no utilities on windows",
)
def test_data_cp_from_cloud_to_storage(
    project_dir: Path,
    remote_project_dir: Path,
    cli_runner: CLIRunner,
    args_data_cp_from_cloud: Callable[..., List[str]],
    bucket: str,
    archive_extension: str,
    extract: bool,
) -> None:
    storage_url = f"storage:neuro-extras-data-cp/{uuid.uuid4()}"
    try:
        src = f"{bucket}/hello.{archive_extension}"
        res = cli_runner(args_data_cp_from_cloud(bucket, src, storage_url, extract))
        assert res.returncode == 0, res

        if extract:
            check_url = storage_url + "/data"
            expected_file = "hello.txt"
        else:
            check_url = storage_url
            expected_file = f"hello.{archive_extension}"

        # BUG: (yartem) cli_runner returns wrong result here putting neuro's debug info
        # to stdout and not putting result of neuro-ls to stdout.
        # So prob cli_runner is to be re-written with subprocess.run
        res = subprocess.run(  # type: ignore
            ["neuro", "ls", check_url], capture_output=True, encoding="utf-8"
        )
        assert res.returncode == 0, res
        assert expected_file in res.stdout, res

    finally:
        res = cli_runner(["neuro", "rm", "-r", storage_url])
        if res.returncode != 0:
            logger.error(f"WARNING: Finalization failed! {res}")
