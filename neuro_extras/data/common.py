"""Module for common functionality and key abstractions related to data copy"""
import abc
import asyncio
import logging
import os
from enum import Flag, auto
from functools import cached_property
from typing import Any, Dict, List, Optional

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


class Copier(metaclass=abc.ABCMeta):
    """Base interface for copying data between a variety of sources"""

    def __init__(self, source: str, destination: str) -> None:
        self.source = source
        self.destination = destination

    @cached_property
    def source_type(self) -> UrlType:
        return UrlType.get_type(self.source)

    @cached_property
    def source_filename(self) -> Optional[str]:
        """Name part of the source url if it is a file, None otherwise"""
        return get_filename_from_url(self.source)

    @cached_property
    def destination_type(self) -> UrlType:
        return UrlType.get_type(self.destination)

    @cached_property
    def destination_filename(self) -> Optional[str]:
        """Name part of the destination url if it is a file, None otherwise"""
        return get_filename_from_url(self.destination)

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
        logger.warn(f"Calling echo instead of actual command!")
        process = await asyncio.create_subprocess_exec("echo", *([command] + args))
        status_code = await process.wait()
        if status_code != 0:
            raise RuntimeError(process.stderr)
