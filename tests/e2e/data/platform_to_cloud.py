import uuid
from typing import List

from ..conftest import (
    CLOUD_DESTINATION_PREFIXES,
    PLATFORM_SOURCE_PREFIXES,
    get_tested_archive_types,
)
from .resources import CopyTestConfig, DataTestResource


def generate_platform_to_cloud_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = get_tested_archive_types()
    run_uuid = uuid.uuid4().hex
    for source_schema, source_prefix in PLATFORM_SOURCE_PREFIXES.items():
        source_archives = [
            DataTestResource(
                schema=source_schema,
                url=f"{source_prefix}/file{ext}",
                is_archive=True,
                file_extension=ext,
            )
            for ext in archive_types
        ]
        source_folder = DataTestResource(
            schema=source_schema,
            url=f"{source_prefix}/",
            is_archive=False,
            file_extension=None,
        )
        for schema, prefix in CLOUD_DESTINATION_PREFIXES.items():
            # tests for copy of local folder
            cloud_folder = DataTestResource(
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
                cloud_archive = DataTestResource(
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
                cloud_extraction_folder = DataTestResource(
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
                cloud_file = DataTestResource(
                    schema=schema,
                    url=f"{prefix}/{run_uuid}/copy/file{archive.file_extension}",
                    is_archive=True,
                    file_extension=archive.file_extension,
                )

                # test for skipping compression
                test_configs.append(
                    CopyTestConfig(
                        source=archive, destination=cloud_file, compress_flag=True
                    )
                )
            if source_archives:
                # use the values for source and destination files
                # from last loop iteration
                test_configs.append(
                    CopyTestConfig(source=archive, destination=cloud_file)
                )
    return test_configs
