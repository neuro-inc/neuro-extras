import abc
import asyncio
import logging
from enum import Flag, auto
from functools import cached_property
from typing import Any, Dict, List

from yarl import URL


logger = logging.getLogger(__name__)


class UrlType(int, Flag):  # type: ignore
    LOCAL = auto()
    AZURE = auto()
    S3 = auto()
    GCS = auto()
    CLOUD = AZURE | S3 | GCS
    STORAGE = auto()
    DISK = auto()
    PLATFORM = STORAGE | DISK
    UNSUPPORTED = ~(LOCAL | CLOUD | PLATFORM)
    SUPPORTED = ~UNSUPPORTED

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, UrlType):
            return bool(self & other)
        return False

    def __hash__(self) -> int:
        return hash(int(self))

    @staticmethod
    def get_scheme_mapping() -> Dict[str, "UrlType"]:
        return {
            "": UrlType.LOCAL,
            "s3": UrlType.S3,
            "gs": UrlType.GCS,
            "azure+https": UrlType.AZURE,
            "storage": UrlType.STORAGE,
            "disk": UrlType.DISK,
        }

    @staticmethod
    def get_type(url: str) -> "UrlType":
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
        logger.info(f"Executing: {[command] + args}")
        logger.warn(f"Calling echo instead of actual command!")
        process = await asyncio.create_subprocess_exec("echo", *([command] + args))
        status_code = await process.wait()
        if status_code != 0:
            raise RuntimeError(process.stderr)


class CLICopier(Copier, CLIRunner):
    """Copier, that uses shell commands to perform copy"""
