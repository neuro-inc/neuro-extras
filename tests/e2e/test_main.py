import asyncio
import base64
import json
import logging
import os
import sys
import textwrap
import time
import uuid
from pathlib import Path
from subprocess import CompletedProcess, check_output
from tempfile import TemporaryDirectory
from time import sleep
from typing import Callable, ContextManager, Iterator, List
from unittest import mock

import pytest
import toml
import yaml
from _pytest.capture import CaptureFixture
from neuromation.cli.const import EX_OK
from neuromation.cli.main import cli as neuro_main

from neuro_extras.main import NEURO_EXTRAS_IMAGE, TEMP_UNPACK_DIR, main as extras_main

from .conftest import CLIRunner, Secret, gen_random_file


logger = logging.getLogger(__name__)

TERM_WIDTH = 80
SEP_BEGIN = "=" * TERM_WIDTH
SEP_END = "-" * TERM_WIDTH


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
def cli_runner(capfd: CaptureFixture[str], project_dir: Path) -> CLIRunner:
    def _run_cli(args: List[str]) -> "CompletedProcess[str]":
        args = args.copy()
        cmd = args.pop(0)
        if cmd not in ("neuro", "neuro-extras"):
            pytest.fail(f"Illegal command: {cmd}")

        logger.info(
            f"Run '{cmd} {' '.join(args)}'",
        )
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
        out, err = out.strip(), err.strip()
        if out:
            logger.debug(f"Stdout:\n{SEP_BEGIN}\n{out}\n{SEP_END}\nStdout finished")
        if err:
            logger.debug(f"Stderr:\n{SEP_BEGIN}\n{err}\n{SEP_END}\nStderr finished")
        return CompletedProcess(
            args=[cmd] + args, returncode=code, stdout=out, stderr=err
        )

    return _run_cli


@pytest.fixture
def repeat_until_success(
    cli_runner: CLIRunner,
) -> Callable[..., "CompletedProcess[str]"]:
    def _f(args: List[str], timeout: int = 5 * 60) -> "CompletedProcess[str]":
        logger.info(f"Waiting {timeout} sec for success of {args}")
        time_started = time.time()
        time_sleep = 5.0
        attempts = 0
        while True:
            if time.time() - time_started > timeout:
                raise ValueError(
                    f"Command {args} couldn't succeed in {attempts} attempts"
                )
            attempts += 1
            try:
                result = cli_runner(args)
                if result.returncode == 0:
                    return result
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                logger.info(f"Command {args}, exception caught: {e}")
            time.sleep(time_sleep)
            time_sleep *= 1.5

    return _f


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
def test_image_build_custom_dockerfile(
    cli_runner: CLIRunner, repeat_until_success: Callable[..., "CompletedProcess[str]"]
) -> None:
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

    result = repeat_until_success(["neuro", "image", "tags", img_name])
    assert tag in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_ignored_files_are_not_copied(
    cli_runner: CLIRunner,
) -> None:
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


def test_data_transfer(
    cli_runner: CLIRunner,
    current_user: str,
    current_cluster: str,
    switch_cluster: Callable[[str], ContextManager[None]],
) -> None:
    # Note: we pushed test image to `neuro-compute`, so it should be a target cluster
    to_cluster = "neuro-compute"
    from_cluster = "onprem-poc"  # can be any other cluster
    assert to_cluster != current_cluster, f"same cluster: {to_cluster}"

    with switch_cluster(from_cluster):
        result = cli_runner(["neuro-extras", "init-aliases"])
        assert result.returncode == 0, result

        src_path = f"copy-src/{str(uuid.uuid4())}"
        result = cli_runner(["neuro", "mkdir", "-p", f"storage:{src_path}"])
        assert result.returncode == 0, result

        dst_path = f"copy-dst/{str(uuid.uuid4())}"

        result = cli_runner(
            [
                "neuro",
                "data-transfer",
                f"storage://{current_cluster}/{current_user}/{src_path}",
                f"storage://{to_cluster}/{current_user}/{dst_path}",
            ]
        )
        assert result.returncode == 0, result

    with switch_cluster(to_cluster):
        result = cli_runner(["neuro", "ls", f"storage:{dst_path}"])
        assert result.returncode == 0, result


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_transfer(
    cli_runner: CLIRunner,
    repeat_until_success: Callable[..., "CompletedProcess[str]"],
    switch_cluster: Callable[[str], ContextManager[None]],
    current_cluster: str,
    current_user: str,
) -> None:
    # Note: we pushed test image to `neuro-compute`, so it should be a target cluster
    to_cluster = "neuro-compute"
    from_cluster = "onprem-poc"  # can be any other cluster
    assert to_cluster != current_cluster, f"same cluster: {to_cluster}"

    with switch_cluster(from_cluster):
        result = cli_runner(["neuro-extras", "init-aliases"])
        assert result.returncode == 0, result

        # WORKAROUND: Fixing 401 Not Authorized because of this problem:
        # https://github.com/neuromation/platform-registry-api/issues/209
        rnd = uuid.uuid4().hex[:6]
        img_name = f"extras-e2e-image-copy-{rnd}"

        tag = str(uuid.uuid4())
        # from_img = f"image://{current_cluster}/{current_user}/{img_name}:{tag}"
        from_img = f"image:{img_name}:{tag}"
        to_img = f"image://{to_cluster}/{current_user}/{img_name}:{tag}"

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

        result = cli_runner(
            ["neuro", "image-build", "-f", str(dockerfile_path), ".", from_img]
        )
        assert result.returncode == 0, result

        result = repeat_until_success(["neuro", "image", "tags", f"image:{img_name}"])
        assert tag in result.stdout

        # Note: this command switches cluster to 'to_cluster'
        result = cli_runner(["neuro", "image-transfer", from_img, to_img])
        assert result.returncode == 0, result

    with switch_cluster(to_cluster):
        result = repeat_until_success(["neuro", "image", "tags", f"image:{img_name}"])
        assert tag in result.stdout


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
def test_seldon_deploy_from_local(
    cli_runner: CLIRunner, repeat_until_success: Callable[..., "CompletedProcess[str]"]
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    pkg_path = Path("pkg")
    result = cli_runner(["neuro", "seldon-init-package", str(pkg_path)])
    assert result.returncode == 0, result
    assert "Copying a Seldon package scaffolding" in result.stdout, result

    assert (pkg_path / "seldon.Dockerfile").exists()

    # TODO (yartem) This part is muted because I'm constantly getting UNAUTHORIZED while
    #  building the image. See https://github.com/neuro-inc/neuro-extras/issues/123
    # tag = str(uuid.uuid4())
    # # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # # https://github.com/neuromation/platform-registry-api/issues/209
    # rnd = uuid.uuid4().hex[:6]
    # img_name = f"image:extras-e2e-seldon-local-{rnd}"
    # img_uri = f"{img_name}:{tag}"
    # result = cli_runner(
    #     ["neuro", "image-build", "-f", "seldon.Dockerfile", str(pkg_path), img_uri]
    # )
    # assert result.returncode == 0, result
    #
    # result = repeat_until_success(["neuro", "image", "tags", img_name])
    # assert tag in result.stdout


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
                "image": NEURO_EXTRAS_IMAGE,
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
                "image": NEURO_EXTRAS_IMAGE,
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "This test weirdly fails on Windows, "
        "see https://github.com/neuro-inc/neuro-extras/issues/128"
    ),
)
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "This test weirdly fails on Windows, "
        "see https://github.com/neuro-inc/neuro-extras/issues/128"
    ),
)
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
    def _f(
        bucket: str,
        src: str,
        dst: str,
        extract: bool,
        compress: bool,
    ) -> List[str]:
        args = ["neuro-extras", "data", "cp", src, dst]
        if (
            src.startswith("storage:")
            or dst.startswith("storage:")
            or src.startswith("disk:")
            or dst.startswith("disk:")
        ):
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
        if compress:
            args.append("-c")
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
        res = cli_runner(args_data_cp_from_cloud(bucket, src, tmp_dir, extract, False))
        assert res.returncode == 0, res

        if extract:
            expected_file = Path(tmp_dir) / "data" / "hello.txt"
            assert "Hello world!" in expected_file.read_text()
        else:
            expected_archive = Path(tmp_dir) / f"hello.{archive_extension}"
            assert expected_archive.is_file()


@pytest.mark.parametrize("bucket", [GCP_BUCKET, AWS_BUCKET])
@pytest.mark.parametrize("archive_extension", ["tar.gz", "tgz", "zip", "tar"])
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows path are not supported yet + no utilities on windows",
)
def test_data_cp_from_cloud_to_local_compress(
    project_dir: Path,
    remote_project_dir: Path,
    cli_runner: CLIRunner,
    args_data_cp_from_cloud: Callable[..., List[str]],
    bucket: str,
    archive_extension: str,
) -> None:
    # TODO: retry because of: https://github.com/neuro-inc/neuro-extras/issues/124
    N = 3
    for attempt in range(1, N + 1):
        try:
            _run_test_data_cp_from_cloud_to_local_compress(
                cli_runner,
                args_data_cp_from_cloud,
                bucket,
                archive_extension,
            )
        except AssertionError as e:
            if "directory not found" in str(e):
                logger.info(f"Attempt {attempt}/{N} failed")
            else:
                raise


def _run_test_data_cp_from_cloud_to_local_compress(
    cli_runner: CLIRunner,
    args_data_cp_from_cloud: Callable[..., List[str]],
    bucket: str,
    archive_extension: str,
) -> None:
    TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=TEMP_UNPACK_DIR.expanduser()) as tmp_dir:
        src = f"{bucket}/hello.{archive_extension}"
        res = cli_runner(
            args_data_cp_from_cloud(
                bucket, src, f"{tmp_dir}/hello.{archive_extension}", False, True
            )
        )
        # XXX: debug info for https://github.com/neuro-inc/neuro-extras/issues/124
        if res.returncode != 0:
            print(f"STDOUT: {res.stdout}")
            print(f"STDERR: {res.stderr}")
        assert res.returncode == 0, res

        expected_file = Path(tmp_dir) / f"hello.{archive_extension}"
        assert expected_file.exists()


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
        res = cli_runner(
            args_data_cp_from_cloud(bucket, src, storage_url, extract, False)
        )
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
        out = check_output(["neuro", "ls", check_url]).decode()
        assert expected_file in out, out

    finally:
        try:
            # Delete disk
            res = cli_runner(["neuro", "rm", "-r", storage_url])
            assert res.returncode == 0, res
        except BaseException as e:
            logger.warning(f"Finalization error: {e}")


@pytest.fixture
def disk(cli_runner: CLIRunner) -> Iterator[str]:
    # Create disk
    res = cli_runner(["neuro", "disk", "create", "100M"])
    assert res.returncode == 0, res
    disk_id = None
    try:
        for line in res.stdout.splitlines():
            elements = line.split()
            if len(elements) >= 3:
                disk_id, storage, uri, *_ = elements
                if disk_id.startswith("disk") and uri.startswith("disk:"):
                    break

        if disk_id is None:
            raise Exception("Unable to locate disk id in neuro output: \n" + res.stdout)

        wait_started = time.time()
        wait_delta = 10.0
        while True:
            if time.time() - wait_started > 5 * 10:
                raise ValueError(f"Could not get disk {disk_id} ready: {res.stdout}")
            res = cli_runner(["neuro", "disk", "get", disk_id])
            assert res.returncode == 0, res
            if "Ready" in res.stdout:
                break
            wait_delta *= 1.5
            time.sleep(wait_delta)

        yield f"disk:{disk_id}"

    finally:
        try:
            # Delete disk
            if disk_id is not None:
                res = cli_runner(["neuro", "disk", "rm", disk_id])
                assert res.returncode == 0, res
        except BaseException as e:
            logger.warning(f"Finalization error: {e}")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows path are not supported yet + no utilities on windows",
)
def test_data_cp_from_cloud_to_disk(
    project_dir: Path,
    remote_project_dir: Path,
    args_data_cp_from_cloud: Callable[..., List[str]],
    cli_runner: CLIRunner,
    disk: str,
) -> None:
    filename = "hello.tar.gz"
    local_folder = "/var/disk"

    src = f"{GCP_BUCKET}/{filename}"
    res = cli_runner(args_data_cp_from_cloud(GCP_BUCKET, src, disk, False, False))
    assert res.returncode == 0, res

    res = cli_runner(
        [
            "neuro",
            "run",
            "-v",
            f"{disk}:{local_folder}:rw",
            "ubuntu",
            f"bash -c 'ls -l {local_folder}/{filename}'",
        ]
    )
    assert res.returncode == 0, res
