import pytest


class MockNeuroClient:
    def __init__(self):
        self._presets = {}

    @property
    def presets(self):
        return self._presets


@pytest.fixture
def mock_client():
    yield MockNeuroClient()
