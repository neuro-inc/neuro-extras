from decimal import Decimal

import pytest
from apolo_sdk import Preset

from apolo_extras.utils import select_job_preset

from .conftest import MockApoloClient


FAKE_PRESETS = {
    "bad": Preset(cpu=1, memory=9999 * 2**20, credits_per_hour=Decimal("5")),
    "expensive": Preset(cpu=2, memory=9999 * 2**20, credits_per_hour=Decimal("15")),
    "cheap": Preset(cpu=20, memory=99999 * 2**20, credits_per_hour=Decimal("14")),
    "cheap_scheduled": Preset(
        cpu=20,
        memory=99999 * 2**20,
        credits_per_hour=Decimal("10"),
        scheduler_enabled=True,
    ),
}


def test_cheapest_preset_is_selected(mock_client: MockApoloClient) -> None:
    mock_client.presets.update(FAKE_PRESETS)
    selected_preset = select_job_preset(
        preset=None, client=mock_client, min_mem=4096, min_cpu=2
    )
    assert selected_preset == "cheap"


@pytest.mark.parametrize("preset", ["bad", "cheap_scheduled"])
def test_user_selection_is_respected(mock_client: MockApoloClient, preset: str) -> None:
    mock_client.presets.update(FAKE_PRESETS)
    selected_preset = select_job_preset(
        preset=preset, client=mock_client, min_mem=4096, min_cpu=2
    )
    assert selected_preset == preset


def test_when_nothing_fits_first_preset_is_used(mock_client: MockApoloClient) -> None:
    presets = {
        "bad": Preset(cpu=1, memory=9999, credits_per_hour=Decimal("5")),
        "gpu": Preset(cpu=4, memory=9999, credits_per_hour=Decimal("15"), nvidia_gpu=1),
    }
    selected_preset = select_job_preset(
        preset=None, client=mock_client, min_mem=4096, min_cpu=2
    )
    mock_client.presets.update(presets)
    assert selected_preset is None
