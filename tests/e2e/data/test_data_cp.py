import logging
import re
import sys
from tempfile import TemporaryDirectory
from typing import Iterable, Iterator, List, Optional

import pytest
from pytest_lazyfixture import lazy_fixture  # type: ignore
from tenacity import retry, stop_after_attempt, stop_after_delay

from .cloud_to_local import generate_cloud_to_local_copy_configs
from .cloud_to_platform import generate_cloud_to_platform_copy_configs
from .local_to_cloud import generate_local_to_cloud_copy_configs
from .local_to_local import generate_local_to_local_copy_configs
from .platform_to_cloud import generate_platform_to_cloud_copy_configs
from .resources import CopyTestConfig, _wrap_test_config_into_yield_fixture
from .utils import _resume_fixture_iterator, _run_command


UUID4_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
DISK_ID_PATTERN = rf"disk-{UUID4_PATTERN}"
DISK_ID_REGEX = re.compile(DISK_ID_PATTERN)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)


TEST_LOCAL_TO_LOCAL = True
TEST_CLOUD_TO_LOCAL = True
TEST_LOCAL_TO_CLOUD = True
TEST_CLOUD_TO_PLATFORM = True
TEST_PLATFORM_TO_CLOUD = True


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


@pytest.fixture(
    params=get_all_data_copy_configs(),
    ids=str,
)
def data_copy_config(request: pytest.FixtureRequest) -> Iterable[CopyTestConfig]:
    fixture_iterator = _wrap_test_config_into_yield_fixture(request=request)
    yield next(fixture_iterator)
    _resume_fixture_iterator(fixture_iterator)


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
@pytest.mark.skipif(sys.platform == "win32", reason="tools don't work on Windows")
def test_data_copy(
    config: CopyTestConfig, tempdir_fixture: str, disk_fixture: str
) -> None:
    config.source.patch_tempdir(tempdir_fixture)
    config.source.patch_disk(disk_fixture)
    config.destination.patch_tempdir(tempdir_fixture)
    config.destination.patch_disk(disk_fixture)
    _run_data_copy_test_from_config(config=config)


@retry(stop=stop_after_attempt(5) | stop_after_delay(6 * 10))
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
