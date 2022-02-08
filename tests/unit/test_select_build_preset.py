from decimal import Decimal

from conftest import MockNeuroClient
from neuro_sdk import Preset

from neuro_extras.utils import select_build_preset


FAKE_PRESETS = {
    "bad": Preset(cpu=1, memory_mb=9999, credits_per_hour=Decimal("5")),
    "expensive": Preset(cpu=2, memory_mb=9999, credits_per_hour=Decimal("15")),
    "cheap": Preset(cpu=20, memory_mb=99999, credits_per_hour=Decimal("14")),
}


def test_cheapest_preset_is_selected(mock_client: MockNeuroClient) -> None:
    mock_client.presets.update(FAKE_PRESETS)
    selected_preset = select_build_preset(
        preset=None, client=mock_client, min_mem=4096, min_cpu=2
    )
    assert selected_preset == "cheap"


def test_user_selection_is_respected(mock_client: MockNeuroClient) -> None:
    mock_client.presets.update(FAKE_PRESETS)
    selected_preset = select_build_preset(
        preset="bad", client=mock_client, min_mem=4096, min_cpu=2
    )
    assert selected_preset == "bad"


def test_when_nothing_fits_first_preset_is_used(mock_client: MockNeuroClient) -> None:
    presets = {
        "bad": Preset(cpu=1, memory_mb=9999, credits_per_hour=Decimal("5")),
        "gpu": Preset(cpu=4, memory_mb=9999, credits_per_hour=Decimal("15"), gpu=1),
    }
    selected_preset = select_build_preset(
        preset=None, client=mock_client, min_mem=4096, min_cpu=2
    )
    mock_client.presets.update(presets)
    assert selected_preset is None
