from typing import Dict, Iterator

import pytest
from neuro_sdk import Client, Preset


class MockNeuroClient(Client):
    def __init__(self) -> None:
        self._presets: Dict[str, Preset] = {}

    @property
    def presets(self) -> Dict[str, Preset]:
        return self._presets


@pytest.fixture
def mock_client() -> Iterator[MockNeuroClient]:
    yield MockNeuroClient._create()
