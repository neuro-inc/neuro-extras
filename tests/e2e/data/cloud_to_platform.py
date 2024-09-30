import logging
import uuid
from typing import List

from ..conftest import (
    CLOUD_SOURCE_PREFIXES,
    PLATFORM_DESTINATION_PREFIXES,
    get_tested_archive_types,
)
from .resources import CopyTestConfig, DataTestResource


logger = logging.getLogger(__name__)


def generate_cloud_to_platform_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = get_tested_archive_types()
    run_uuid = uuid.uuid4().hex
    for schema, cloud_url in CLOUD_SOURCE_PREFIXES.items():
        dir_copy_should_fail = schema in ("http", "https")
        dir_copy_fail_reason = "Copy from HTTP(S) dir is unsupported"
        for platform_schema, platform_prefix in PLATFORM_DESTINATION_PREFIXES.items():
            cloud_folder = DataTestResource(
                schema=schema,
                url=f"{cloud_url}/",
                is_archive=False,
                file_extension=None,
            )
            platform_copy_folder = DataTestResource(
                schema=platform_schema,
                url=f"{platform_prefix}/{run_uuid}/copy/{schema}/folder/",
                is_archive=False,
                file_extension=None,
            )

            for ext in archive_types:
                # test for copy of remote archive into local one
                cloud_archive = DataTestResource(
                    schema=schema,
                    url=f"{cloud_url}/file{ext}",
                    is_archive=True,
                    file_extension=ext,
                )
                platform_archive = DataTestResource(
                    schema=platform_schema,
                    url=f"{platform_prefix}/{run_uuid}/compress/{schema}/file{ext}",
                    is_archive=True,
                    file_extension=ext,
                )

                test_configs.append(
                    CopyTestConfig(
                        source=cloud_archive,
                        destination=platform_archive,
                        compress_flag=True,
                    )
                )
                # test for extraction of cloud archive into local dir
                platform_extract_folder = DataTestResource(
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
                platform_compressed_archive = DataTestResource(
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
            if archive_types:
                # use the values for source and destination files
                # from last loop iteration
                test_configs.append(
                    CopyTestConfig(
                        source=cloud_archive,
                        destination=platform_archive,
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
