"""Module for common functionality and key abstractions related to data copy"""
import abc
import asyncio
import logging
from enum import Flag, auto
from functools import cached_property
from typing import Any, Dict, List

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

    @cached_property
    def source_type(self) -> UrlType:
        return UrlType.get_type(self.source)

    @cached_property
    def destination_type(self) -> UrlType:
        return UrlType.get_type(self.destination)

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


class CLICopier(Copier, CLIRunner):
    """Copier, that uses shell commands to perform copy"""

    # TODO: (A.K.) Move from CLICopier to CLIRunner inheritance or composition
