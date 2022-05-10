"""Module for common functionality and key abstractions related to data copy"""
import abc
import asyncio
import logging
import os
import re
from enum import Flag, auto
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Tuple

from neuro_sdk import Client
from yarl import URL


logger = logging.getLogger(__name__)


class UrlType(int, Flag):  # type: ignore
    """Enum type for handling source/destination types

    Supports comparisons between particular type and its category:
    >>> assert UrlType.GCS == UrlType.CLOUD
    >>> assert UrlType.LOCAL == (~UrlType.UNSUPPORTED)
    """

    LOCAL_FS = auto()
    AZURE = auto()
    S3 = auto()
    GCS = auto()
    HTTP = auto()
    HTTPS = auto()
    WEB = HTTP | HTTPS
    CLOUD = AZURE | S3 | GCS | WEB
    STORAGE = auto()
    DISK = auto()
    PLATFORM = STORAGE | DISK
    UNSUPPORTED = ~(LOCAL_FS | CLOUD | PLATFORM)
    SUPPORTED = ~UNSUPPORTED

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, UrlType):
            return bool(self & other)
        return False

    def __hash__(self) -> int:
        return hash(int(self))

    @staticmethod
    def get_scheme_mapping() -> Dict[str, "UrlType"]:
        """Get supported mapping of url schema to UrlType"""
        return {
            "": UrlType.LOCAL_FS,
            "s3": UrlType.S3,
            "gs": UrlType.GCS,
            "azure+https": UrlType.AZURE,
            "storage": UrlType.STORAGE,
            "disk": UrlType.DISK,
            "http": UrlType.HTTP,
            "https": UrlType.HTTPS,
        }

    @staticmethod
    def get_type(url: str) -> "UrlType":
        """Detect UrlType by checking url schema"""
        scheme_mapping = UrlType.get_scheme_mapping()
        url_scheme = URL(url).scheme
        return scheme_mapping.get(url_scheme, UrlType.UNSUPPORTED)


class Copier(metaclass=abc.ABCMeta):
    """Base interface for copying data between a variety of sources"""

    def __init__(self, source: str, destination: str) -> None:
        self.source = source
        self.destination = destination
        self.source_type = UrlType.get_type(source)
        self.destination_type = UrlType.get_type(destination)
        self.source_filename = get_filename_from_url(source)
        self.destination_filename = get_filename_from_url(destination)

    async def perform_copy(self) -> str:
        """Copy data from self.source to self.destination
        and return path to the copied resource"""
        raise NotImplementedError


class CLIRunner:
    """Utility class for running shell commands"""

    async def run_command(self, command: str, args: List[str]) -> None:
        """Execute command with args

        If resulting statuscode is non-zero, RuntimeError is thrown
        with stderr as a message.
        """
        logger.info(f"Executing: {[command] + args}")
        # logger.warn(f"Calling echo instead of actual command!")
        # process = await asyncio.create_subprocess_exec("echo", *([command] + args))
        process = await asyncio.create_subprocess_exec(command, *args)
        status_code = await process.wait()
        if status_code != 0:
            raise RuntimeError(process.stderr)


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


def strip_filename_from_url(url: str) -> str:
    """Return a url to the parent folder if the url points to a file"""
    filename = get_filename_from_url(url)
    if filename is None:
        return url
    pattern = f"{filename}$"
    return re.sub(pattern=pattern, repl="", string=url)


def ensure_parent_folder_exists(local_url: str) -> None:
    folder_name = strip_filename_from_url(local_url)
    logger.info(f"Creating folder for {folder_name}")
    Path(folder_name).mkdir(exist_ok=True, parents=True)


def parse_resource_spec(url: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    """Parse schema, resource_id, subpath, mode from platform resource"""
    parts = url.split(":")
    if parts[-1] in ("ro", "rw"):
        mode = parts[-1]
        schema, resource_id, subpath, _ = parse_resource_spec(":".join(parts[:-1]))
    elif len(parts) == 2:
        schema, resouce_path = parts
        if not resouce_path.startswith("/"):
            resource_path_parts = resouce_path.split("/")
            resource_id = resource_path_parts[0]
            subpath = (
                ("/" + "/".join(resource_path_parts[1:]))
                if len(resource_path_parts) > 1
                else None
            )
        elif resouce_path.startswith("//"):
            resource_path_parts = resouce_path.split("/")
            resource_id = "/".join(resource_path_parts[:5])
            subpath = (
                ("/" + "/".join(resource_path_parts[5:]))
                if len(resource_path_parts) > 5
                else None
            )
        elif resouce_path.startswith("/"):
            resource_path_parts = resouce_path.split("/")
            resource_id = "/".join(resource_path_parts[:3])
            subpath = (
                ("/" + "/".join(resource_path_parts[3:]))
                if len(resource_path_parts) > 4
                else None
            )
        mode = None
    else:
        raise ValueError(f"Coudn't parse resource spec from {url}")
    return schema, resource_id, subpath, mode


def get_default_preset(neuro_client: Client) -> str:
    """Get default preset name via Neu.ro client"""
    return next(iter(neuro_client.presets.keys()))


def provide_temp_dir(
    dir: Path = Path.home() / ".neuro-tmp",
) -> TemporaryDirectory:  # type: ignore
    """Provide temp directory"""
    dir.mkdir(exist_ok=True, parents=True)
    return TemporaryDirectory(dir=dir)
