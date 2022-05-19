from pathlib import Path
from typing import List

from .resources import TEMPDIR_PREFIX, CopyTestConfig, Resource
from .utils import get_tested_archive_types


def generate_local_to_local_copy_configs() -> List[CopyTestConfig]:
    assets_root = (Path(__file__).parent.parent.parent / "assets" / "data").resolve()
    archive_types = get_tested_archive_types()
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