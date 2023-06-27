import logging
from typing import List

from ..conftest import CLOUD_SOURCE_PREFIXES, get_tested_archive_types
from .resources import TEMPDIR_PREFIX, CopyTestConfig, DataTestResource


logger = logging.getLogger(__name__)


def generate_cloud_to_local_copy_configs() -> List[CopyTestConfig]:
    test_configs: List[CopyTestConfig] = []
    archive_types = get_tested_archive_types()

    for schema, cloud_url in CLOUD_SOURCE_PREFIXES.items():
        cloud_folder = DataTestResource(
            schema=schema,
            url=f"{cloud_url}/",
            is_archive=False,
            file_extension=None,
        )
        local_copy_folder = DataTestResource(
            schema="local",
            url=f"{TEMPDIR_PREFIX}/copy/{schema}/folder/",
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
            local_archive = DataTestResource(
                schema="local",
                url=f"{TEMPDIR_PREFIX}/copy/{schema}/file{ext}",
                is_archive=True,
                file_extension=ext,
            )

            test_configs.append(
                CopyTestConfig(
                    source=cloud_archive,
                    destination=local_archive,
                    compress_flag=True,
                )
            )
            # test for extraction of cloud archive into local dir
            local_extract_folder = DataTestResource(
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

            local_compressed_archive = DataTestResource(
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
        if archive_types:
            # use the values for cloud_archive and local_archive
            # from last loop iteration
            test_configs.append(
                CopyTestConfig(
                    source=cloud_archive,
                    destination=local_archive,
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
