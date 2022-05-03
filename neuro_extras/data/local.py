"""Module for handling copy operations, that can be run locally

Contains:
- LocalToLocalCopier
- LocalToCloudCopier
- CloudToLocalCopier
"""
import logging
import tempfile
from pathlib import Path
from typing import Optional, Tuple, Type

from neuro_extras.data.fs import LocalFSCopier
from neuro_extras.data.web import WebCopier

from .archive import ArchiveManager, ArchiveType
from .azure import AzureCopier
from .common import Copier, UrlType
from .gcs import GCSCopier
from .s3 import S3Copier
from .utils import get_filename_from_url


logger = logging.getLogger(__name__)


class BaseLocalCopier(Copier):
    """Base class for copiers, which can be executed locally"""

    def __init__(
        self,
        source: str,
        destination: str,
        compress: bool = False,
        extract: bool = False,
        temp_dir: Path = Path(tempfile.gettempdir()),
    ) -> None:
        super().__init__(source=source, destination=destination)
        self.archive_manager = ArchiveManager()
        self.compress = compress
        self.extract = extract
        self.temp_dir = temp_dir

    @staticmethod
    def get_copier(source: str, destination: str, type: UrlType) -> Copier:
        """Get copier of proper type to copy from"""
        destination_copier_mapping = {
            UrlType.S3: S3Copier,
            UrlType.AZURE: AzureCopier,
            UrlType.GCS: GCSCopier,
            UrlType.HTTP: WebCopier,
            UrlType.HTTPS: WebCopier,
            UrlType.LOCAL_FS: LocalFSCopier,
        }
        cls: Type[Copier] = destination_copier_mapping[type]
        return cls(source=source, destination=destination)

    @staticmethod
    def check_both_are_archives(
        source: str, destination: str
    ) -> Tuple[bool, Optional[bool]]:
        """Check if both urls point to archives and whether they are of the same type

        Returns tuple (both_are_archives: bool, both_of_the_same_type: Optional[bool])
        """
        source_filename = get_filename_from_url(source)
        destination_filename = get_filename_from_url(destination)
        if None in (source_filename, destination_filename):
            # at least one is a directory
            return (False, None)
        source_archive_type = ArchiveType.get_type(
            archive=Path(source_filename)  # type: ignore
        )
        destination_archive_type = ArchiveType.get_type(
            archive=Path(destination_filename)  # type: ignore
        )
        if ArchiveType.UNSUPPORTED in (source_archive_type, destination_archive_type):
            # at least one is not a supported archive
            return (False, None)
        return (True, source_archive_type == destination_archive_type)


class LocalToLocalCopier(BaseLocalCopier):
    """Copier, that can copy data from and to local storage

    Supports compression and extraction.
    """

    async def _recompress(self) -> str:
        both_are_archives, both_of_same_type = BaseLocalCopier.check_both_are_archives(
            self.source, self.destination
        )
        if not both_are_archives:
            raise ValueError(
                "Can't perform recompression - "
                "source and destination should be both supported archives"
            )
        if both_of_same_type:
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_extraction_destination = self.temp_dir / "extracted"
        extracted_folder = await self.archive_manager.extract(
            source=Path(self.source), destination=temp_extraction_destination
        )
        new_archive = await self.archive_manager.compress(
            source=extracted_folder, destination=Path(self.destination)
        )
        return str(new_archive)

    async def _extract(self) -> str:
        archive_name = get_filename_from_url(self.source)
        if archive_name is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        extracted_folder = await self.archive_manager.extract(
            source=Path(self.source), destination=Path(self.destination)
        )
        return str(extracted_folder)

    async def _compress(self) -> str:
        archive_name = get_filename_from_url(self.destination)
        if archive_name is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        compressed_file = await self.archive_manager.compress(
            source=Path(self.source), destination=Path(self.destination)
        )
        return str(compressed_file)

    async def _copy(self) -> str:
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source, destination=self.destination, type=UrlType.LOCAL_FS
        )
        return await copier_implementation.perform_copy()

    async def perform_copy(self) -> str:
        """Perform copy from local fs to local fs.

        Delegates copy implementation to appropriate LocalFSCopier.
        Uses ArchiveManager to handle compression/extraction.
        """
        if self.extract and self.compress:
            return await self._recompress()
        elif self.extract:
            return await self._extract()
        elif self.compress:
            return await self._compress()
        else:
            return await self._copy()


class LocalToCloudCopier(BaseLocalCopier):
    """Copier, that can copy data from local storage to a storage bucket

    Supports compression and extraction (temp_dir is used to store intermediate results)
    """

    async def _recompress_and_copy(self) -> str:
        raise NotImplementedError

    async def _extract_and_copy(self) -> str:
        raise NotImplementedError

    async def _compress_and_copy(self) -> str:
        raise NotImplementedError

    async def _copy(self) -> str:
        raise NotImplementedError

    async def perform_copy(self) -> str:
        """Perform copy from local to cloud.

        Delegates copy implementation to appropriate Copier (S3, Azure, GCS, Web).
        Uses ArchiveManager to handle compression/extraction.
        """
        # TODO: replace with calls to corresponing private methods
        if self.extract:
            archive_name = get_filename_from_url(self.source)
            if archive_name is None:
                raise ValueError(f"Can't infer archive type from source {self.source}")
            extracted_folder = await self.archive_manager.extract(
                source=Path(self.source), destination=self.temp_dir
            )
            copy_source = str(extracted_folder)
        elif self.compress:
            archive_name = get_filename_from_url(self.destination)
            if archive_name is None:
                raise ValueError(
                    f"Can't infer archive type from destination {self.destination}"
                )
            compressed_file = await self.archive_manager.compress(
                source=Path(self.source), destination=self.temp_dir / archive_name
            )
            copy_source = str(compressed_file)
        else:
            copy_source = self.source
        copier_implementation = BaseLocalCopier.get_copier(
            source=copy_source, destination=self.destination, type=self.destination_type
        )
        return await copier_implementation.perform_copy()


class CloudToLocalCopier(BaseLocalCopier):
    """Copier, that can copy data from cloud storage to local fs

    Supports compression and extraction (temp_dir is used to store intermediate results)
    """

    async def _copy_and_recompress(self) -> str:
        raise NotImplementedError

    async def _copy_and_extract(self) -> str:
        raise NotImplementedError

    async def _copy_and_compress(self) -> str:
        raise NotImplementedError

    async def _copy(self) -> str:
        raise NotImplementedError

    async def perform_copy(self) -> str:
        # TODO: replace with calls to corresponing private methods
        if self.extract:
            archive_name = get_filename_from_url(self.source)
            if archive_name is None:
                raise ValueError(f"Can't infer archive type from source {self.source}")
            temp_archive = str(self.temp_dir / archive_name)
            copier_implementation = BaseLocalCopier.get_copier(
                source=self.source, destination=temp_archive, type=self.source_type
            )
            archive = await copier_implementation.perform_copy()
            extraction_result = await self.archive_manager.extract(
                source=Path(archive), destination=Path(self.destination)
            )
            return str(extraction_result)
        elif self.compress:
            archive_name = get_filename_from_url(self.destination)
            if archive_name is None:
                raise ValueError(
                    f"Can't infer archive type from destination {self.destination}"
                )
            copier_implementation = BaseLocalCopier.get_copier(
                source=self.source,
                destination=str(self.temp_dir),
                type=self.source_type,
            )
            directory = await copier_implementation.perform_copy()
            compression_result = await self.archive_manager.compress(
                source=Path(directory), destination=Path(self.destination)
            )
            return str(compression_result)
        else:
            copier_implementation = BaseLocalCopier.get_copier(
                source=self.source, destination=self.destination, type=self.source_type
            )
            return await copier_implementation.perform_copy()
