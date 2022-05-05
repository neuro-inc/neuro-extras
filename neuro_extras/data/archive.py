"""Module for archive management operations (compression and extraction)"""
import abc
import logging
from enum import Flag, auto
from pathlib import Path
from typing import Any, Dict, List

from .common import CLIRunner, get_filename_from_url


logger = logging.getLogger(__name__)


class ArchiveType(int, Flag):  # type: ignore
    """Int Flag for archive types

    Supports fuzzy checks:
    >>> assert ArchiveType.TAR_GZ == ArchiveType.TAR
    >>> assert (ArchiveType.GZ | ArchiveType.ZIP) == ArchiveType.SUPPORTED
    """

    TAR_PLAIN = auto()
    TAR_GZ = auto()
    TAR_BZ = auto()
    TAR = TAR_PLAIN | TAR_GZ | TAR_BZ
    GZ = auto()
    ZIP = auto()
    SUPPORTED = TAR | GZ | ZIP
    UNSUPPORTED = ~(SUPPORTED)

    @staticmethod
    def get_extensions_for_type(type: "ArchiveType") -> List[str]:
        """Get list of file extensions, that correspond
        to the provided archive type"""
        return [
            ext
            for ext, type_ in ArchiveType.get_extension_mapping().items()
            if type_ == type
        ]

    @staticmethod
    def get_extension_mapping() -> Dict[str, "ArchiveType"]:
        """Get mapping from file extension to ArchiveType"""
        return {
            ".tar.gz": ArchiveType.TAR_GZ,
            ".tgz": ArchiveType.TAR_GZ,
            ".tar.bz2": ArchiveType.TAR_BZ,
            ".tbz2": ArchiveType.TAR_BZ,
            ".tbz": ArchiveType.TAR_BZ,
            ".tar": ArchiveType.TAR_PLAIN,
            ".gz": ArchiveType.GZ,
            ".zip": ArchiveType.ZIP,
        }

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ArchiveType):
            return bool(self & other)
        return False

    def __hash__(self) -> int:
        return hash(int(self))

    @staticmethod
    def get_type(archive: Path) -> "ArchiveType":
        """Determine archive type from file extension"""
        suffixes = archive.suffixes[-2:]  # keep only at most 2 suffixes
        if not suffixes:
            return ArchiveType.UNSUPPORTED
        if "".join(suffixes) in ArchiveType.get_extension_mapping():
            # match longest possible suffix first
            return ArchiveType.get_extension_mapping()["".join(suffixes)]
        else:
            # try to match last suffix
            return ArchiveType.get_extension_mapping().get(
                suffixes[-1], ArchiveType.UNSUPPORTED
            )


class ArchiveManager(metaclass=abc.ABCMeta):
    """Interface for archive management"""

    async def compress(self, source: Path, destination: Path) -> Path:
        """Compress source into destination"""
        raise NotImplementedError

    async def extract(self, source: Path, destination: Path) -> Path:
        """Extract source into destination"""
        raise NotImplementedError


class TarManager(ArchiveManager, CLIRunner):
    """Utility class for handling tar archives"""

    async def compress(self, source: Path, destination: Path) -> Path:
        """Compress source into destination using tar command"""
        command = "tar"
        archive_type = ArchiveType.get_type(destination)
        if archive_type == (~ArchiveType.TAR):
            raise ValueError(
                f"Can't compress into {destination} with TarManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.TAR)}"
            )
        mapping = {
            ArchiveType.TAR_GZ: "zcf",
            ArchiveType.TAR_BZ: "jcf",
            ArchiveType.TAR_PLAIN: "cf",
        }
        subcommand = mapping[archive_type]
        args = [
            subcommand,
            str(destination),
            f"--exclude={destination.name}",
            str(source),
        ]
        await self.run_command(command=command, args=args)
        return destination

    async def extract(self, source: Path, destination: Path) -> Path:
        """Extract source into destination using tar command"""
        command = "tar"
        archive_type = ArchiveType.get_type(source)
        if archive_type == (~ArchiveType.TAR):
            raise ValueError(
                f"Can't extract {source} with TarManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.TAR)}"
            )
        mapping = {
            ArchiveType.TAR_GZ: "zxvf",
            ArchiveType.TAR_BZ: "jxvf",
            ArchiveType.TAR_PLAIN: "xvf",
        }
        subcommand = mapping[archive_type]
        args = [subcommand, str(source), f"-C", str(destination)]
        destination.mkdir(exist_ok=True, parents=True)
        await self.run_command(command=command, args=args)
        return destination


class GzipManager(ArchiveManager, CLIRunner):
    """Utility class for handling gzip archives"""

    async def compress(self, source: Path, destination: Path) -> Path:
        """Compress source into destination using gzip command"""
        command = "gzip"
        archive_type = ArchiveType.get_type(destination)
        if archive_type == (~ArchiveType.GZ):
            raise ValueError(
                f"Can't compress into {destination} with GzipManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.GZ)}"
            )
        args = ["-r", str(destination), str(source)]
        await self.run_command(command=command, args=args)
        return destination

    async def extract(self, source: Path, destination: Path) -> Path:
        """Extract source into destination using gunzip command"""
        command = "gunzip"
        archive_type = ArchiveType.get_type(destination)
        if archive_type == (~ArchiveType.GZ):
            raise ValueError(
                f"Can't extract {destination} with GzipManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.GZ)}"
            )
        args = ["--keep", str(source), str(destination)]
        destination.mkdir(exist_ok=True, parents=True)
        await self.run_command(command=command, args=args)
        return destination


class ZipManager(ArchiveManager, CLIRunner):
    """Utility class for handling zip archives"""

    async def compress(self, source: Path, destination: Path) -> Path:
        """Compress source into destination using zip command"""
        command = "zip"
        archive_type = ArchiveType.get_type(destination)
        if archive_type == (~ArchiveType.ZIP):
            raise ValueError(
                f"Can't compress into {destination} with ZipManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.ZIP)}"
            )
        # check if works as expected
        args = ["-r", str(destination), str(source)]
        await self.run_command(command=command, args=args)
        return destination

    async def extract(self, source: Path, destination: Path) -> Path:
        """Extract source into destination using unzip command"""
        command = "unzip"
        archive_type = ArchiveType.get_type(destination)
        if archive_type == (~ArchiveType.ZIP):
            raise ValueError(
                f"Can't extract {destination} with ZipManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.ZIP)}"
            )
        args = [str(source), "-d", str(destination)]
        destination.mkdir(exist_ok=True, parents=True)
        await self.run_command(command=command, args=args)
        return destination


def _get_archive_manager(archive: Path) -> ArchiveManager:
    """Resolve appropriate archive manager"""
    mapping = {
        ArchiveType.TAR: TarManager(),
        ArchiveType.GZ: GzipManager(),
        ArchiveType.ZIP: ZipManager(),
    }
    archive_type = ArchiveType.get_type(archive)
    if archive_type == ArchiveType.UNSUPPORTED:
        supported_extensions = list(ArchiveType.get_extension_mapping())
        raise ValueError(
            f"Unsupported archive type for file {archive}, "
            f"supported types are {supported_extensions}"
        )
    return next(manager for type, manager in mapping.items() if type == archive_type)


async def copy(source: Path, destination: Path) -> Path:
    """Copy source into destination"""
    command = "cp"
    args = [str(source), str(destination)]
    runner = CLIRunner()
    await runner.run_command(command=command, args=args)
    return destination


async def compress(source: Path, destination: Path) -> Path:
    """Compress source into destination while
    inferring arhive type from destination"""
    source_filename = get_filename_from_url(str(source))
    destination_filename = get_filename_from_url(str(destination))
    if source_filename is not None and destination_filename is not None:
        source_type = ArchiveType.get_type(source)
        destination_type = ArchiveType.get_type(destination)
        both_archives = ArchiveType.UNSUPPORTED not in (
            source_type,
            destination_type,
        )
        same_type = source_type == destination_type
        if both_archives and same_type:
            logger.info(
                "Skipping compression step - "
                "source is already archive of the same type"
            )
            return await copy(source=source, destination=destination)

    manager_implementation = _get_archive_manager(destination)
    logger.info(
        f"Compressing {source} into {destination} "
        f"with {manager_implementation.__class__.__name__}"
    )
    return await manager_implementation.compress(source=source, destination=destination)


async def extract(source: Path, destination: Path) -> Path:
    """Extract source into destination while
    inferring arhive type from source"""
    manager_implementation = _get_archive_manager(source)
    logger.info(
        f"Extracting {source} into {destination} "
        f"with {manager_implementation.__class__.__name__}"
    )
    return await manager_implementation.extract(source=source, destination=destination)
