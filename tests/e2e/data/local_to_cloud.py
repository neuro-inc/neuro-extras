import os
import uuid
from pathlib import Path
from typing import List

from .resources import CopyTestConfig, Resource
from .utils import get_tested_archive_types


def generate_local_to_cloud_copy_configs() -> List[CopyTestConfig]:
    """Generate test configs for local to cloud data copy

    This method is used to allow for selective test generation,
    instead of generating all possible file/flag combinations as the
    number of tests grows exponentially with the addition of new
    supported platforms
    """
    test_configs: List[CopyTestConfig] = []

    assets_root = (Path(__file__).parent.parent.parent / "assets" / "data").resolve()
    archive_types = get_tested_archive_types()
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
