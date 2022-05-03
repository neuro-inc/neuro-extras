import logging
from pathlib import Path
from tempfile import TemporaryDirectory

from neuro_sdk import Client


logger = logging.getLogger(__name__)


def get_default_preset(neuro_client: Client) -> str:
    return next(iter(neuro_client.presets.keys()))


def provide_temp_dir(
    dir: Path = Path.home() / ".neuro-tmp",
) -> TemporaryDirectory:  # type: ignore
    # TODO: (A.K.) use .neuro-tmp/ or update tests
    return TemporaryDirectory(dir=dir)
