import logging
import uuid
from typing import List

from .resources import DISK_PREFIX, CopyTestConfig, Resource
from .utils import get_tested_archive_types


logger = logging.getLogger(__name__)


def generate_cloud_to_platform_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = get_tested_archive_types()
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
