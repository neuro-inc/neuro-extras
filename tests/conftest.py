from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

import pytest


@pytest.fixture
def tmp_file(mode: str = "w") -> Iterator[Path]:
    fp = Path(NamedTemporaryFile(mode).name)
    try:
        yield fp
    finally:
        fp.unlink()
