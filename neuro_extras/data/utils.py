import logging
import os
from tempfile import TemporaryDirectory
from typing import Optional

from neuro_sdk import Client
from yarl import URL

from .common import UrlType


logger = logging.getLogger(__name__)


def get_filename_from_url(url: str) -> Optional[str]:
    """Get filename from url, or None if directory url is passed

    Uses pathlib for local files and URL otherwise
    """
    url_type = UrlType.get_type(url)
    if url_type == UrlType.LOCAL_FS:
        # use pathlib
        head, tail = os.path.split(url)
        return tail if tail else None
    else:
        parsed_url = URL(url)
        parts = parsed_url.path.split("/")
        if parts:
            return parts[-1] if parts[-1] else None
        else:
            return None


def get_default_preset(neuro_client: Client) -> str:
    return next(iter(neuro_client.presets.keys()))


def provide_temp_dir() -> TemporaryDirectory:  # type: ignore
    # TODO: (A.K.) use .neuro-tmp/ or update tests
    return TemporaryDirectory()
