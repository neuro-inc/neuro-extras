import base64
import json
import logging
import os
import re
import sys
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, ContextManager, Iterator, List
from unittest import mock

import pytest
import toml
import yaml

from neuro_extras.common import NEURO_EXTRAS_IMAGE
from neuro_extras.data import TEMP_UNPACK_DIR

from .conftest import TESTED_ARCHIVE_TYPES, CLIRunner


root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)


UUID4_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
DISK_ID_PATTERN = fr"disk-{UUID4_PATTERN}"
DISK_ID_REGEX = re.compile(DISK_ID_PATTERN)

GCP_BUCKET = "gs://mlops-ci-e2e"
AWS_BUCKET = "s3://cookiecutter-e2e"
AZURE_BUCKET = "azure+https://neuromlops.blob.core.windows.net/cookiecutter-e2e"
HTTP_BUCKET = "http://s3.amazonaws.com/data.neu.ro/cookiecutter-e2e"
HTTPS_BUCKET = "https://s3.amazonaws.com/data.neu.ro/cookiecutter-e2e"


def test_init_aliases(cli_runner: CLIRunner) -> None:
    toml_path = Path(".neuro.toml")
    assert not toml_path.exists()

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result
    assert "Added aliases to" in result.stdout, result

    assert toml_path.exists()


@pytest.mark.serial
def test_data_transfer(
    cli_runner: CLIRunner,
    current_user: str,
    switch_cluster: Callable[[str], ContextManager[None]],
) -> None:
    # Note: data-transfer runs copying job on dst_cluster and
    # we pushed test image to src_cluster, so it should be a target cluster
    src_cluster = os.environ["NEURO_CLUSTER_SECONDARY"]
    dst_cluster = os.environ["NEURO_CLUSTER"]

    with switch_cluster(src_cluster):
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
                f"storage:{src_path}",  # also, full src uri is supported
                f"storage://{dst_cluster}/{current_user}/{dst_path}",
            ]
        )
        assert result.returncode == 0, result

        del_result = cli_runner(["neuro", "rm", "-r", f"storage:{src_path}"])
        assert del_result.returncode == 0, result

    with switch_cluster(dst_cluster):
        result = cli_runner(["neuro", "ls", f"storage:{dst_path}"])
        assert result.returncode == 0, result

        del_result = cli_runner(["neuro", "rm", "-r", f"storage:{dst_path}"])
        assert del_result.returncode == 0, result


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
    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    img_name = f"image:extras-e2e-seldon-local-{rnd}"
    img_uri = f"{img_name}:{tag}"
    result = cli_runner(
        ["neuro", "image-build", "-f", "seldon.Dockerfile", str(pkg_path), img_uri]
    )
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "image", "tags", img_name])
    assert tag in result.stdout


def test_config_save_registry_auth_locally(cli_runner: CLIRunner) -> None:
    result = cli_runner(
        ["neuro-extras", "config", "save-registry-auth", ".docker.json"]
    )
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
        use_temp_dir: bool,
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
            elif bucket.startswith("azure+https://"):
                args.extend(
                    [
                        "-e",
                        "AZURE_SAS_TOKEN=secret:azure_sas_token",
                    ]
                )
            elif bucket.startswith("https://") or bucket.startswith("http://"):
                # No additional arguments required
                pass
            else:
                raise NotImplementedError(bucket)
        if extract:
            args.append("-x")
        if compress:
            args.append("-c")
        if use_temp_dir:
            args.append("-t")
        logger.info("args = %s", args)
        return args

    return _f


@pytest.mark.parametrize(
    "bucket", [AWS_BUCKET, GCP_BUCKET, AZURE_BUCKET, HTTP_BUCKET, HTTPS_BUCKET]
)
@pytest.mark.parametrize("archive_extension", TESTED_ARCHIVE_TYPES)
@pytest.mark.parametrize("extract", [True, False])
@pytest.mark.parametrize("use_temp_dir", [True, False])
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
    use_temp_dir: bool,
) -> None:
    TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=TEMP_UNPACK_DIR.expanduser()) as tmp_dir:
        src = f"{bucket}/hello.{archive_extension}"
        dst = tmp_dir
        if not extract:
            dst = f"{tmp_dir}/hello.{archive_extension}"

        res = cli_runner(
            args_data_cp_from_cloud(bucket, src, dst, extract, False, use_temp_dir)
        )
        assert res.returncode == 0, res

        if extract:
            expected_file = Path(dst) / "data" / "hello.txt"
            assert "Hello world!" in expected_file.read_text()
        else:
            expected_archive = Path(dst)
            assert expected_archive.is_file()


@pytest.mark.parametrize(
    "bucket", [GCP_BUCKET, AWS_BUCKET, AZURE_BUCKET, HTTP_BUCKET, HTTPS_BUCKET]
)
@pytest.mark.parametrize("use_temp_dir", [True, False])
@pytest.mark.parametrize(
    "from_extension, to_extension",
    list(zip(TESTED_ARCHIVE_TYPES, TESTED_ARCHIVE_TYPES[1:] + TESTED_ARCHIVE_TYPES[:1]))
    + [(TESTED_ARCHIVE_TYPES[0], TESTED_ARCHIVE_TYPES[0])],
)
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
    from_extension: str,
    to_extension: str,
    use_temp_dir: bool,
) -> None:
    # TODO: retry because of: https://github.com/neuro-inc/neuro-extras/issues/124
    N = 3
    FLAKY_ERROR_MESSAGES = ["file changed as we read it", "directory not found"]
    for attempt in range(1, N + 1):
        try:
            logger.info(f"Trying attempt {attempt}/{N}")
            _run_test_data_cp_from_cloud_to_local_compress(
                cli_runner,
                args_data_cp_from_cloud,
                bucket,
                from_extension,
                to_extension,
                use_temp_dir,
            )
            return
        except AssertionError as e:
            if any(s in str(e) for s in FLAKY_ERROR_MESSAGES):
                logger.warning(f"Failed attempt {attempt}/{N}: {e}")
                continue
            raise


def _run_test_data_cp_from_cloud_to_local_compress(
    cli_runner: CLIRunner,
    args_data_cp_from_cloud: Callable[..., List[str]],
    bucket: str,
    from_extension: str,
    to_extension: str,
    use_temp_dir: bool,
) -> None:
    TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=TEMP_UNPACK_DIR.expanduser()) as tmp_dir:
        src = f"{bucket}/hello.{from_extension}"
        dst = f"{tmp_dir}/hello.{to_extension}"

        res = cli_runner(
            args_data_cp_from_cloud(bucket, src, dst, False, True, use_temp_dir)
        )
        # XXX: debug info for https://github.com/neuro-inc/neuro-extras/issues/124
        if res.returncode != 0:
            print(f"STDOUT: {res.stdout}")
            print(f"STDERR: {res.stderr}")
        assert res.returncode == 0, res

        if from_extension == to_extension:
            # if src and dst archive types are the same - compression should be skipped.
            assert "Skipping compression step" in res.stdout, res

        expected_file = Path(tmp_dir) / f"hello.{to_extension}"
        assert expected_file.exists()


@pytest.mark.parametrize(
    "bucket", [GCP_BUCKET, AWS_BUCKET, AZURE_BUCKET, HTTP_BUCKET, HTTPS_BUCKET]
)
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
    dst = f"storage:neuro-extras-data-cp/{uuid.uuid4()}"
    try:
        src = f"{bucket}/hello.{archive_extension}"
        if not extract:
            dst = f"{dst}/hello.{archive_extension}"

        res = cli_runner(
            args_data_cp_from_cloud(bucket, src, dst, extract, False, True)
        )
        assert res.returncode == 0, res

        if extract:
            glob_pattern = f"{dst}/data/*"
            expected_file = "/data/hello.txt"
        else:
            glob_pattern = f"{dst}*"
            expected_file = f"{Path(dst).name}"

        # BUG: (yartem) cli_runner returns wrong result here putting neuro's debug info
        # to stdout and not putting result of neuro-ls to stdout.
        # So prob cli_runner is to be re-written with subprocess.run
        out = cli_runner(["neuro", "storage", "glob", glob_pattern])
        assert expected_file in out.stdout, out.stdout

    finally:
        try:
            # Delete disk
            res = cli_runner(["neuro", "rm", "-r", dst])
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
        output_lines = "\n".join(res.stdout.splitlines())

        search = DISK_ID_REGEX.search(output_lines)
        if search:
            disk_id = search.group()
        else:
            raise Exception("Can't find disk ID in neuro output: \n" + res.stdout)

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
    dst = f"{disk}:/intermediate/{filename}"
    res = cli_runner(args_data_cp_from_cloud(GCP_BUCKET, src, dst, False, False, False))
    assert res.returncode == 0, res

    res = cli_runner(
        [
            "neuro",
            "run",
            "-v",
            f"{disk}:{local_folder}:rw",
            "ubuntu",
            f"bash -c 'ls -l {local_folder}/intermediate/{filename}'",
        ]
    )
    assert res.returncode == 0, res
