import abc
import logging
from enum import Flag, auto
from pathlib import Path
from typing import Any, Dict, List

from .common import CLIRunner, get_filename_from_url


logger = logging.getLogger(__name__)


class ArchiveType(int, Flag):  # type: ignore
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


class BaseArchiveManager(metaclass=abc.ABCMeta):
    """Interface for archive management"""

    async def compress(self, source: Path, destination: Path) -> Path:
        raise NotImplementedError

    async def extract(self, source: Path, destination: Path) -> Path:
        raise NotImplementedError


class TarManager(BaseArchiveManager, CLIRunner):
    async def compress(self, source: Path, destination: Path) -> Path:
        command = "tar"
        archive_type = ArchiveType.get_type(destination)
        if archive_type == (~ArchiveType.TAR):
            raise ValueError(
                f"Can't compress into {destination} with TarManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.TAR)}"
            )
        if archive_type == ArchiveType.TAR_GZ:
            args = [
                "zcf",
                str(destination),
                f"--exclude={destination.name}",
                str(source),
            ]
        elif archive_type == ArchiveType.TAR_BZ:
            args = [
                "jcf",
                str(destination),
                f"--exclude={destination.name}",
                str(source),
            ]
        else:
            args = [
                "cf",
                str(destination),
                f"--exclude={destination.name}",
                str(source),
            ]
        await self.run_command(command=command, args=args)
        return destination

    async def extract(self, source: Path, destination: Path) -> Path:
        command = "tar"
        archive_type = ArchiveType.get_type(source)
        if archive_type == (~ArchiveType.TAR):
            raise ValueError(
                f"Can't extract {source} with TarManager: "
                f"unsupported archive type {archive_type.name}. "
                f"Supported types: "
                f"{ArchiveType.get_extensions_for_type(ArchiveType.TAR)}"
            )
        if archive_type == ArchiveType.TAR_GZ:
            args = ["zxvf", str(source), f"-C", str(destination)]
        elif archive_type == ArchiveType.TAR_BZ:
            args = ["jxvf", str(source), f"-C", str(destination)]
        else:
            args = ["xvf", str(source), f"-C", str(destination)]
        await self.run_command(command=command, args=args)
        return destination


class GzipManager(BaseArchiveManager, CLIRunner):
    async def compress(self, source: Path, destination: Path) -> Path:
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
        await self.run_command(command=command, args=args)
        return destination


class ZipManager(BaseArchiveManager, CLIRunner):
    async def compress(self, source: Path, destination: Path) -> Path:
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
        await self.run_command(command=command, args=args)
        return destination


class ArchiveManager(BaseArchiveManager):
    """Utility class for compression and extraction operations"""

    @staticmethod
    def get_archive_manager(archive: Path) -> BaseArchiveManager:
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
        return next(
            manager for type, manager in mapping.items() if type == archive_type
        )

    async def _copy(self, source: Path, destination: Path) -> Path:
        command = "cp"
        args = [str(source), str(destination)]
        runner = CLIRunner()
        await runner.run_command(command=command, args=args)
        return destination

    async def compress(self, source: Path, destination: Path) -> Path:
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
                return await self._copy(source=source, destination=destination)

        manager_implementation = ArchiveManager.get_archive_manager(destination)
        logger.info(
            f"Compressing {source} into {destination} "
            f"with {manager_implementation.__class__.__name__}"
        )
        return await manager_implementation.compress(
            source=source, destination=destination
        )

    async def extract(self, source: Path, destination: Path) -> Path:
        manager_implementation = ArchiveManager.get_archive_manager(source)
        logger.info(
            f"Extracting {source} into {destination} "
            f"with {manager_implementation.__class__.__name__}"
        )
        return await manager_implementation.extract(
            source=source, destination=destination
        )
