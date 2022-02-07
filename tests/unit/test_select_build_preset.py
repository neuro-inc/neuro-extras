from decimal import Decimal

import pytest
from neuro_sdk import Preset

from neuro_extras.image import _select_build_preset


@pytest.mark.asyncio
async def test_cheapest_preset_is_selected(mock_client):
    presets = {
        "bad": Preset(cpu=1, memory_mb=9999, credits_per_hour=Decimal("5")),
        "expensive": Preset(cpu=2, memory_mb=9999, credits_per_hour=Decimal("15")),
        "cheap": Preset(cpu=20, memory_mb=99999, credits_per_hour=Decimal("14")),
    }
    mock_client.presets.update(presets)
    selected_preset = await _select_build_preset(mock_client)
    assert selected_preset == "cheap"


@pytest.mark.asyncio
async def test_when_nothing_fits_first_preset_is_used(mock_client):
    presets = {
        "bad": Preset(cpu=1, memory_mb=9999, credits_per_hour=Decimal("5")),
        "gpu": Preset(cpu=4, memory_mb=9999, credits_per_hour=Decimal("15"), gpu=1),
    }
    selected_preset = await _select_build_preset(mock_client)
    mock_client.presets.update(presets)
    assert selected_preset is None
