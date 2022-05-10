import dataclasses
import logging
import os
import re
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Iterator, List, Optional, Tuple

import pytest
from pytest_lazyfixture import lazy_fixture  # type: ignore
from tenacity import retry, stop_after_attempt, stop_after_delay
from yarl import URL

from neuro_extras.data.azure import _build_sas_url, _patch_azure_url_for_rclone
from neuro_extras.data.common import (
    get_filename_from_url,
    parse_resource_spec,
    strip_filename_from_url,
)

from .conftest import run_cli


UUID4_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
DISK_ID_PATTERN = rf"disk-{UUID4_PATTERN}"
DISK_ID_REGEX = re.compile(DISK_ID_PATTERN)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

DISK_PREFIX = "<DISK_PREFIX>"
TEMPDIR_PREFIX = "<TEMPDIR_PREFIX>"

TEST_LOCAL_TO_LOCAL = True
TEST_CLOUD_TO_LOCAL = True
TEST_LOCAL_TO_CLOUD = True
TEST_CLOUD_TO_PLATFORM = True
TEST_PLATFORM_TO_CLOUD = True


@dataclasses.dataclass
class Resource:
    schema: str
    url: str
    is_archive: bool
    file_extension: Optional[str] = None

    def remove(self) -> None:
        """Remove the resource at self.url"""
        removal_handlers = {
            "local": _remove_local,
            "s3": _remove_s3,
            "azure+https": _remove_azure,
            "gs": _remove_gcs,
            "storage": _remove_storage,
        }
        if self.schema in removal_handlers:
            logger.info(f"Removing {self.url}")
            removal_handlers[self.schema](self.url)
        else:
            logger.warning(f"No cleanup handler for schema '{self.schema}'")

    def exists(self) -> bool:
        """Check if the resource exists and is of appropriate type (dir or file)"""
        handlers = {
            "local": _local_resource_exists,
            "s3": _s3_resource_exists,
            "gs": _gcs_resource_exists,
            "azure+https": _azure_resource_exist,
            "storage": _storage_resource_exists,
            "disk": _disk_exists,
        }
        if self.schema in handlers:
            logger.info(f"Checking if {self.url} exists")
            return handlers[self.schema](self)
        else:
            logger.error(f"Check for '{self.schema}' schema is not implemented!")
            return False

    def patch_disk(self, disk: str) -> None:
        """Patch disk url containing DISK_PREFIX with specified disk id"""
        if self.schema == "disk":
            patched = re.sub(DISK_PREFIX, disk, self.url)
            logger.info(f"Patched {self.url} into {patched}")
            self.url = patched

    def patch_tempdir(self, dir: str) -> None:
        if self.schema == "local":
            patched = re.sub(TEMPDIR_PREFIX, dir, self.url)
            logger.info(f"Patched {self.url} into {patched}")
            self.url = patched


def _remove_local(url: str) -> None:
    returncode, stdout, stderr = _run_command("rm", ["-rvf", url])
    assert returncode == 0, stderr


def _remove_gcs(url: str) -> None:
    returncode, stdout, stderr = _run_command("gsutil", ["-m", "rm", "-r", url])
    assert returncode == 0, stderr


def _remove_s3(url: str) -> None:
    returncode, stdout, stderr = _run_command("aws", ["s3", "rm", "--recursive", url])
    assert returncode == 0, stderr


def _remove_storage(url: str) -> None:
    returncode, stdout, stderr = _run_command("neuro", ["storage", "rm", "-r", url])
    assert returncode == 0 or "Not Found" in stderr


def _remove_azure(url: str) -> None:
    azure_sas_url = _build_sas_url(url)
    patched_url = _patch_azure_url_for_rclone(url)
    returncode, stdout, stderr = _run_command(
        "rclone", ["delete", "--azureblob-sas-url", azure_sas_url, patched_url]
    )
    assert returncode == 0, stderr


def _local_resource_exists(resource: Resource) -> bool:
    path = Path(resource.url)
    path_exists = path.exists()
    valid_type = (
        path.is_file() if resource.file_extension is not None else path.is_dir()
    )
    return path_exists and valid_type


def _s3_resource_exists(resource: Resource) -> bool:
    if resource.file_extension:
        url = URL(resource.url)
        bucket_name = url.host
        filename = "/".join(url.parts[1:])
        command = "aws"
        args = ["s3api", "head-object", "--bucket", bucket_name, "--key", filename]
    else:
        command = "aws"
        args = [
            "s3",
            "ls",
            resource.url,
        ]
    returcode, stdout, stederr = _run_command(command, args)  # type: ignore
    return returcode == 0


def _storage_resource_exists(resource: Resource) -> bool:
    command = "neuro"
    args = ["storage", "ls", "-l", strip_filename_from_url(resource.url)]
    returncode, stdout, stderr = _run_command(command, args)
    if resource.file_extension is None:
        check_is_successful = True
    else:
        stdout = stdout if stdout else ""
        filename: str = get_filename_from_url(resource.url)  # type: ignore
        check_is_successful = filename in stdout
    return returncode == 0 and check_is_successful


def _disk_exists(resource: Resource) -> bool:
    command = "neuro"
    schema, disk_id, path_on_disk, _ = parse_resource_spec(resource.url)
    mountpoint = "/var/mnt"
    path_in_job = f"{mountpoint}{path_on_disk if path_on_disk else '/'}"
    args = [
        "job",
        "run",
        "-v",
        f"{schema}:{disk_id}:{mountpoint}",
        "busybox",
        "--",
        "stat",
        path_in_job,
    ]
    returncode, stdout, stderr = _run_command(command, args)
    return returncode == 0


def _gcs_resource_exists(resource: Resource) -> bool:
    command = "gsutil"
    args = [
        "ls",
        resource.url,
    ]
    returncode, _, _ = _run_command(command, args)
    return returncode == 0


def _azure_resource_exist(resource: Resource) -> bool:
    command = "rclone"
    args = [
        "-q",
        "--azureblob-sas-url",
        _build_sas_url(resource.url),
        "lsf",
        _patch_azure_url_for_rclone(resource.url),
    ]
    returncode, stdout, stderr = _run_command(command, args)
    return returncode == 0 and bool(stdout)


@dataclasses.dataclass
class CopyTestConfig:
    source: Resource
    destination: Resource
    extract_flag: bool = False
    compress_flag: bool = False
    # if set, force testcase to be expected to fail
    should_fail: Optional[bool] = None
    # if should_fail is set - provide reason
    fail_reason: Optional[str] = None

    def as_command(self, minimized: bool = False) -> List[str]:
        if minimized:
            command = []
        else:
            command = ["data", "cp"] + self.get_extra_args()
        if self.extract_flag:
            command.append("-x")
        if self.compress_flag:
            command.append("-c")
        if minimized:
            source_filepart = (
                self.source.file_extension if self.source.file_extension else "/"
            )
            source_url = f"{self.source.schema}:***{source_filepart}"
            destination_filepart = (
                self.destination.file_extension
                if self.destination.file_extension
                else "/"
            )
            destination_url = f"{self.destination.schema}:***{destination_filepart}"
        else:
            source_url = self.source.url
            destination_url = self.destination.url
        command += [
            source_url,
            destination_url,
        ]
        return command

    def __str__(self) -> str:
        return " ".join(self.as_command(minimized=True))

    def get_extra_args(self) -> List[str]:
        schemas = (self.source.schema, self.destination.schema)
        if not (("storage" in schemas) or ("disk" in schemas)):
            return []
        extra_args = []
        if "gs" in schemas:
            extra_args += [
                "-v",
                "secret:neuro-extras-gcp:/gcp-creds.txt",
                "-e",
                "GOOGLE_APPLICATION_CREDENTIALS=/gcp-creds.txt",
            ]
        if "azure+https" in schemas:
            extra_args += ["-e", "AZURE_SAS_TOKEN=secret:azure_sas_token"]
        if "s3" in schemas:
            extra_args += [
                "-v",
                "secret:neuro-extras-aws:/aws-creds.txt",
                "-e",
                "AWS_CONFIG_FILE=/aws-creds.txt",
            ]
        return extra_args


def get_all_data_copy_configs() -> List[CopyTestConfig]:
    configs = []
    if TEST_LOCAL_TO_LOCAL:
        configs += generate_local_to_local_copy_configs()
    if TEST_CLOUD_TO_LOCAL:
        configs += generate_cloud_to_local_copy_configs()
    if TEST_LOCAL_TO_CLOUD:
        configs += generate_local_to_cloud_copy_configs()
    if TEST_CLOUD_TO_PLATFORM:
        configs += generate_cloud_to_platform_copy_configs()
    if TEST_PLATFORM_TO_CLOUD:
        configs += generate_platform_to_cloud_copy_configs()
    return configs


def _run_command(cmd: str, args: List[str]) -> Tuple[int, str, str]:
    logger.info(f"Running: {[cmd] + args}")
    result = run_cli([cmd] + args)
    return result.returncode, result.stdout, result.stderr


def archive_types_are_compatible(
    source: Optional[str], destination: Optional[str]
) -> bool:
    groups = [
        {".tar.gz", ".tgz"},
        {".tar.bz2", ".tbz", ".tbz2"},
        {".tar"},
        {".zip"},
        {".gzip", ".gz"},
    ]
    return any(source in group and destination in group for group in groups)


def generate_local_to_cloud_copy_configs() -> List[CopyTestConfig]:
    """Generate test configs for local to cloud data copy

    This method is used to allow for selective test generation,
    instead of generating all possible file/flag combinations as the
    number of tests grows exponentially with the addition of new
    supported platforms
    """
    test_configs: List[CopyTestConfig] = []

    assets_root = (Path(__file__).parent.parent / "assets" / "data").resolve()
    archive_types = [".tar.gz", ".tgz", ".zip", ".tar", ".tbz"]
    local_archives = [
        Resource(
            "local",
            str(assets_root / f"file{ext}"),
            is_archive=True,
            file_extension=ext,
        )
        for ext in archive_types
    ]
    local_folder = Resource(
        "local", str(assets_root) + os.sep, is_archive=False, file_extension=None
    )
    destination_prefixes = {
        "s3": "s3://cookiecutter-e2e/data_cp",
        "gs": "gs://mlops-ci-e2e/data_cp",
        "azure+https": "azure+https://neuromlops.blob.core.windows.net/cookiecutter-e2e/data_cp",  # noqa: E501
        "http": "http://s3.amazonaws.com/data.neu.ro/cookiecutter-e2e",
        "https": "https://s3.amazonaws.com/data.neu.ro/cookiecutter-e2e",
    }
    run_uuid = uuid.uuid4().hex

    for schema, prefix in destination_prefixes.items():
        # tests for copy of local folder
        cloud_folder = Resource(
            schema=schema,
            url=f"{prefix}/{run_uuid}/copy/",
            is_archive=False,
            file_extension=None,
        )
        test_configs.append(
            CopyTestConfig(source=local_folder, destination=cloud_folder)
        )

        # tests for compression of local folder
        for archive_type in archive_types:
            cloud_archive = Resource(
                schema=schema,
                url=f"{prefix}/{run_uuid}/compress/file{archive_type}",
                is_archive=True,
                file_extension=archive_type,
            )
            # gzip does not work properly with folders
            # tar should be used instead
            compression_should_fail = archive_type == ".gz"
            compression_fail_reason = "gzip does not support folders properly"
            test_configs.append(
                CopyTestConfig(
                    source=local_folder,
                    destination=cloud_archive,
                    compress_flag=True,
                    should_fail=compression_should_fail,
                    fail_reason=compression_fail_reason,
                )
            )

        # tests for copy of local files
        for archive in local_archives:
            # test for extraction of archive
            cloud_extraction_folder = Resource(
                schema=schema,
                url=f"{prefix}/{run_uuid}/extract/{archive.file_extension}/",
                is_archive=False,
                file_extension=None,
            )
            test_configs.append(
                CopyTestConfig(
                    source=archive,
                    destination=cloud_extraction_folder,
                    extract_flag=True,
                )
            )

            # test for file copy
            cloud_file = Resource(
                schema=schema,
                url=f"{prefix}/{run_uuid}/copy/file{archive.file_extension}",
                is_archive=True,
                file_extension=archive.file_extension,
            )
            test_configs.append(CopyTestConfig(source=archive, destination=cloud_file))

            # test for skipping compression
            test_configs.append(
                CopyTestConfig(
                    source=archive, destination=cloud_file, compress_flag=True
                )
            )

    return test_configs


def generate_local_to_local_copy_configs() -> List[CopyTestConfig]:
    assets_root = (Path(__file__).parent.parent / "assets" / "data").resolve()
    archive_types = [".tar.gz", ".tgz", ".zip", ".tar", ".tbz"]
    test_configs: List[CopyTestConfig] = []
    plain_copy = [
        CopyTestConfig(
            source=Resource(
                schema="local",
                url=str(assets_root / f"file{ext}"),
                file_extension=ext,
                is_archive=True,
            ),
            destination=Resource(
                schema="local",
                url=f"{TEMPDIR_PREFIX}/copy/local/file{ext}",
                file_extension=ext,
                is_archive=True,
            ),
        )
        for ext in archive_types
    ]
    test_configs += plain_copy
    return test_configs


def generate_cloud_to_local_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = [".tar.gz", ".tgz", ".zip", ".tar", ".tbz"]
    cloud_source_prefixes = {
        "gs": "gs://mlops-ci-e2e/assets/data",
        "s3": "s3://cookiecutter-e2e/assets/data",
        "azure+https": "azure+https://neuromlops.blob.core.windows.net/cookiecutter-e2e/assets/data",  # noqa: E501
        "http": "http://s3.amazonaws.com/cookiecutter-e2e/assets/data",
        "https": "https://s3.amazonaws.com/cookiecutter-e2e/assets/data",
    }
    for schema, cloud_url in cloud_source_prefixes.items():
        cloud_folder = Resource(
            schema=schema,
            url=f"{cloud_url}/",
            is_archive=False,
            file_extension=None,
        )
        local_copy_folder = Resource(
            schema="local",
            url=f"{TEMPDIR_PREFIX}/copy/{schema}/folder/",
            is_archive=False,
            file_extension=None,
        )
        for ext in archive_types:
            # test for copy of remote archive into local one
            cloud_archive = Resource(
                schema=schema,
                url=f"{cloud_url}/file{ext}",
                is_archive=True,
                file_extension=ext,
            )
            local_archive = Resource(
                schema="local",
                url=f"{TEMPDIR_PREFIX}/copy/{schema}/file{ext}",
                is_archive=True,
                file_extension=ext,
            )
            test_configs.append(
                CopyTestConfig(
                    source=cloud_archive,
                    destination=local_archive,
                )
            )
            test_configs.append(
                CopyTestConfig(
                    source=cloud_archive,
                    destination=local_archive,
                    compress_flag=True,
                )
            )
            # test for extraction of cloud archive into local dir
            local_extract_folder = Resource(
                schema="local",
                url=f"{TEMPDIR_PREFIX}/extract/{schema}/{ext}/",
                is_archive=False,
            )
            test_configs.append(
                CopyTestConfig(
                    source=cloud_archive,
                    destination=local_extract_folder,
                    extract_flag=True,
                )
            )

            # test for compression of cloud folder into local archive

            local_compressed_archive = Resource(
                schema="local",
                url=f"{TEMPDIR_PREFIX}/compress/{schema}/file{ext}",
                is_archive=True,
                file_extension=ext,
            )

            # test for dir copy
            dir_copy_should_fail = schema in ("http", "https")
            dir_copy_fail_reason = "Copy from HTTP(S) dir is unsupported"
            test_configs.append(
                CopyTestConfig(
                    source=cloud_folder,
                    destination=local_compressed_archive,
                    compress_flag=True,
                    should_fail=dir_copy_should_fail,
                    fail_reason=dir_copy_fail_reason,
                )
            )
        test_configs.append(
            CopyTestConfig(
                source=cloud_folder,
                destination=local_copy_folder,
                should_fail=dir_copy_should_fail,
                fail_reason=dir_copy_fail_reason,
            )
        )

    logger.info(f"Generated cloud to local tests: {test_configs}")
    return test_configs


def generate_cloud_to_platform_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = [".tar.gz", ".tgz", ".zip", ".tar", ".tbz"]
    cloud_source_prefixes = {
        "gs": "gs://mlops-ci-e2e/assets/data",
        "s3": "s3://cookiecutter-e2e/assets/data",
        "azure+https": "azure+https://neuromlops.blob.core.windows.net/cookiecutter-e2e/assets/data",  # noqa: E501
        "http": "http://s3.amazonaws.com/cookiecutter-e2e/assets/data",
        "https": "https://s3.amazonaws.com/cookiecutter-e2e/assets/data",
    }
    destination_prefixes = {
        "storage": "storage:e2e/data_cp",
        "disk": f"{DISK_PREFIX}/data_cp",
    }
    run_uuid = uuid.uuid4().hex
    for schema, cloud_url in cloud_source_prefixes.items():
        dir_copy_should_fail = schema in ("http", "https")
        dir_copy_fail_reason = "Copy from HTTP(S) dir is unsupported"
        for platform_schema, platform_prefix in destination_prefixes.items():
            cloud_folder = Resource(
                schema=schema,
                url=f"{cloud_url}/",
                is_archive=False,
                file_extension=None,
            )
            platform_copy_folder = Resource(
                schema=platform_schema,
                url=f"{platform_prefix}/{run_uuid}/copy/{schema}/folder/",
                is_archive=False,
                file_extension=None,
            )

            for ext in archive_types:
                # test for copy of remote archive into local one
                cloud_archive = Resource(
                    schema=schema,
                    url=f"{cloud_url}/file{ext}",
                    is_archive=True,
                    file_extension=ext,
                )
                platform_archive = Resource(
                    schema=platform_schema,
                    url=f"{platform_prefix}/{run_uuid}/compress/{schema}/file{ext}",
                    is_archive=True,
                    file_extension=ext,
                )
                test_configs.append(
                    CopyTestConfig(
                        source=cloud_archive,
                        destination=platform_archive,
                    )
                )
                test_configs.append(
                    CopyTestConfig(
                        source=cloud_archive,
                        destination=platform_archive,
                        compress_flag=True,
                    )
                )
                # test for extraction of cloud archive into local dir
                platform_extract_folder = Resource(
                    schema=platform_schema,
                    url=f"{platform_prefix}/{run_uuid}/extract/{schema}/{ext}/",
                    is_archive=False,
                )
                test_configs.append(
                    CopyTestConfig(
                        source=cloud_archive,
                        destination=platform_extract_folder,
                        extract_flag=True,
                    )
                )

                # test for compression of cloud folder into local archive
                platform_compressed_archive = Resource(
                    schema=platform_schema,
                    url=f"{platform_prefix}/{run_uuid}/compress/{schema}/file{ext}",
                    is_archive=True,
                    file_extension=ext,
                )

                test_configs.append(
                    CopyTestConfig(
                        source=cloud_folder,
                        destination=platform_compressed_archive,
                        compress_flag=True,
                        should_fail=dir_copy_should_fail,
                        fail_reason=dir_copy_fail_reason,
                    )
                )

            test_configs.append(
                CopyTestConfig(
                    source=cloud_folder,
                    destination=platform_copy_folder,
                    should_fail=dir_copy_should_fail,
                    fail_reason=dir_copy_fail_reason,
                )
            )

    logger.info(f"Generated cloud to platform tests: {test_configs}")
    return test_configs


def generate_platform_to_cloud_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = [".tar.gz", ".tgz", ".zip", ".tar", ".tbz"]
    destination_prefixes = {
        "gs": "gs://mlops-ci-e2e/data_cp",
        "s3": "s3://cookiecutter-e2e/data_cp",
        "azure+https": "azure+https://neuromlops.blob.core.windows.net/cookiecutter-e2e/data_cp",  # noqa: E501
        "http": "http://s3.amazonaws.com/cookiecutter-e2e/data_cp",
        "https": "https://s3.amazonaws.com/cookiecutter-e2e/data_cp",
    }
    source_prefixes = {
        "storage": "storage:e2e/assests/data",
        "disk": f"disk:disk-17e231e0-6065-4331-a2be-67933ae98f6a/assets/data",
    }
    run_uuid = uuid.uuid4().hex
    for source_schema, source_prefix in source_prefixes.items():
        source_archives = [
            Resource(
                schema=source_schema,
                url=f"{source_prefix}/file{ext}",
                is_archive=True,
                file_extension=ext,
            )
            for ext in archive_types
        ]
        source_folder = Resource(
            schema=source_schema,
            url=f"{source_prefix}/",
            is_archive=False,
            file_extension=None,
        )
        for schema, prefix in destination_prefixes.items():
            # tests for copy of local folder
            cloud_folder = Resource(
                schema=schema,
                url=f"{prefix}/{run_uuid}/copy/",
                is_archive=False,
                file_extension=None,
            )
            test_configs.append(
                CopyTestConfig(source=source_folder, destination=cloud_folder)
            )

            # tests for compression of local folder
            for archive_type in archive_types:
                cloud_archive = Resource(
                    schema=schema,
                    url=f"{prefix}/{run_uuid}/compress/file{archive_type}",
                    is_archive=True,
                    file_extension=archive_type,
                )
                # gzip does not work properly with folders
                # tar should be used instead
                compression_should_fail = archive_type == ".gz"
                compression_fail_reason = "gzip does not support folders properly"
                test_configs.append(
                    CopyTestConfig(
                        source=source_folder,
                        destination=cloud_archive,
                        compress_flag=True,
                        should_fail=compression_should_fail,
                        fail_reason=compression_fail_reason,
                    )
                )

            # tests for copy of local files
            for archive in source_archives:
                # test for extraction of archive
                cloud_extraction_folder = Resource(
                    schema=schema,
                    url=f"{prefix}/{run_uuid}/extract/{archive.file_extension}/",
                    is_archive=False,
                    file_extension=None,
                )
                test_configs.append(
                    CopyTestConfig(
                        source=archive,
                        destination=cloud_extraction_folder,
                        extract_flag=True,
                    )
                )

                # test for file copy
                cloud_file = Resource(
                    schema=schema,
                    url=f"{prefix}/{run_uuid}/copy/file{archive.file_extension}",
                    is_archive=True,
                    file_extension=archive.file_extension,
                )
                test_configs.append(
                    CopyTestConfig(source=archive, destination=cloud_file)
                )

                # test for skipping compression
                test_configs.append(
                    CopyTestConfig(
                        source=archive, destination=cloud_file, compress_flag=True
                    )
                )
    return test_configs


def _resume_fixture_iterator(iterator: Iterator[Any]) -> None:
    try:
        next(iterator)
    except StopIteration:
        pass


@pytest.fixture(
    params=get_all_data_copy_configs(),
    ids=str,
)
def data_copy_config(request: pytest.FixtureRequest) -> Iterable[CopyTestConfig]:
    fixture_iterator = _wrap_test_config_into_yield_fixture(request=request)
    yield next(fixture_iterator)
    _resume_fixture_iterator(fixture_iterator)


def _wrap_test_config_into_yield_fixture(
    request: pytest.FixtureRequest,
) -> Iterator[CopyTestConfig]:
    """Wrap test config into a yield fixture with automatic
    destination cleaning afterwards"""
    config: CopyTestConfig = request.param  # type: ignore
    yield config
    logger.info(f"Cleaning up destination after '{config.as_command(minimized=True)}'")
    config.destination.remove()


@pytest.fixture(scope="session")
def disk_fixture() -> Iterator[str]:
    """Provide temporary disk, which will be removed upon test end

    WARNING: when used with pytest-parallel,
    disk will be created one per each worker!
    """
    # Create disk
    returncode, stdout, _ = _run_command("neuro", ["disk", "create", "100M"])
    assert returncode == 0
    disk_id = None
    try:
        output_lines = "\n".join(stdout.splitlines())

        search = DISK_ID_REGEX.search(output_lines)
        if search:
            disk_id = search.group()
        else:
            raise Exception("Can't find disk ID in neuro output: \n" + stdout)
        logger.info(f"Created disk {disk_id}")
        yield f"disk:{disk_id}"

    finally:
        logger.info(f"Removing disk {disk_id}")
        try:
            # Delete disk
            if disk_id is not None:
                returncode, _, _ = _run_command("neuro", ["disk", "rm", disk_id])
                assert returncode == 0
        except BaseException as e:
            logger.warning(f"Finalization error: {e}")


@pytest.fixture(scope="session")
def tempdir_fixture() -> Iterator[str]:
    """Provide temporary folder, which will be removed upon test end

    WARNING: when used with pytest-parallel,
    folder will be created one per each worker!
    """
    with TemporaryDirectory() as tmpdir:
        logger.info(f"Created tempdir: {tmpdir}")
        yield tmpdir
    logger.info(f"Removed tempdir: {tmpdir}")


@pytest.mark.parametrize(
    argnames="config", argvalues=[lazy_fixture("data_copy_config")]
)
def test_data_copy(
    config: CopyTestConfig, tempdir_fixture: str, disk_fixture: str
) -> None:
    config.source.patch_tempdir(tempdir_fixture)
    config.source.patch_disk(disk_fixture)
    config.destination.patch_tempdir(tempdir_fixture)
    config.destination.patch_disk(disk_fixture)
    _run_data_copy_test_from_config(config=config)


@retry(stop=stop_after_attempt(2) | stop_after_delay(3 * 10))
def _run_data_copy_test_from_config(config: CopyTestConfig) -> None:
    logger.info(f"Running test from {repr(config)}")
    destination = config.destination
    source = config.source
    extract_flag = config.extract_flag
    compress_flag = config.compress_flag
    supported_destination_schemas = {
        "local": {"local", "s3", "gs", "azure+https", "storage", "disk"},
        "http": {"local", "storage", "disk"},
        "https": {"local", "storage", "disk"},
        "s3": {"local", "storage", "disk"},
        "gs": {"local", "storage", "disk"},
        "azure+https": {"local", "storage", "disk"},
        "storage": {"local", "s3", "gs", "azure+https", "disk"},
        "disk": {
            "local",
            "s3",
            "gs",
            "azure+https",
            "storage",
        },
    }
    supported_resource_combination = (
        destination.schema in supported_destination_schemas[source.schema]
    )
    bad_extraction = not source.is_archive and extract_flag
    bad_compression = not destination.is_archive and compress_flag
    invalid_flag_combination = bad_extraction or bad_compression
    should_succeed = (
        supported_resource_combination
        and not invalid_flag_combination
        and not config.should_fail
    )
    reasons_to_fail = []
    if not supported_resource_combination:
        reasons_to_fail.append(
            f"copy from '{source.schema}' schema "
            f"to '{destination.schema}' schema is unsupported."
        )
    if invalid_flag_combination:
        reasons_to_fail.append(
            f"invalid combination of filetypes and compress/extract flags."
        )
    if config.should_fail and config.fail_reason:
        reasons_to_fail.append(config.fail_reason)
    returncode, stdout, stderr = _run_command("neuro-extras", config.as_command())
    succeeded = returncode == 0
    verb = "should" if should_succeed else "should not"
    assert_fail_message = f"'{config.as_command(minimized=True)} {verb} succeed."
    if not should_succeed:
        assert_fail_message += f" Reasons: {'; '.join(reasons_to_fail)}"
    assert succeeded == should_succeed, assert_fail_message
    if not should_succeed:
        return
    if should_succeed:
        should_exist_message = f"Destination {destination} "
        "should exist if copy succeeded"
        assert destination.exists(), should_exist_message
    both_archives = source.is_archive and destination.is_archive
    compatible_archives = archive_types_are_compatible(
        source.file_extension, destination.file_extension
    )
    should_skip_compression = compress_flag and both_archives and compatible_archives
    if should_skip_compression:
        assert "Skipping compression step" in stdout, "Should skip compression"
    # TODO: add check for recompression
