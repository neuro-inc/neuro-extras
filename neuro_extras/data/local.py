"""Module for handling copy operations, that can be run locally

Contains:
- LocalToLocalCopier
- LocalToCloudCopier
- CloudToLocalCopier
"""
import logging
import tempfile
from pathlib import Path
from typing import Type

from neuro_extras.data.fs import LocalFSCopier
from neuro_extras.data.web import WebCopier

from .archive import ArchiveType, compress, extract
from .azure import AzureCopier
from .common import Copier, UrlType
from .gcs import GCSCopier
from .s3 import S3Copier


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

    def _can_skip_recompression(self) -> bool:
        """Check if both urls point to archives of same type"""
        if None in (self.source_filename, self.destination_filename):
            # at least one is a directory
            return False
        source_archive_type = ArchiveType.get_type(
            archive=Path(self.source_filename)  # type: ignore
        )
        destination_archive_type = ArchiveType.get_type(
            archive=Path(self.destination_filename)  # type: ignore
        )
        if ArchiveType.UNSUPPORTED in (source_archive_type, destination_archive_type):
            # at least one is not a supported archive
            return False
        return source_archive_type == destination_archive_type

    def _ensure_recompression_possible(self) -> None:
        """Raise error if recompression is not possible"""
        if None in (self.source_filename, self.source_filename):
            # at least one is a directory
            raise ValueError("Recompression is unsupported for directories")
        source_archive_type = ArchiveType.get_type(
            archive=Path(self.source_filename)  # type: ignore
        )
        destination_archive_type = ArchiveType.get_type(
            archive=Path(self.destination_filename)  # type: ignore
        )
        if ArchiveType.UNSUPPORTED in (source_archive_type, destination_archive_type):
            raise ValueError("Recompression is not possible for unsupported archives")


class LocalToLocalCopier(BaseLocalCopier):
    """Copier, that can copy data from and to local storage

    Supports compression and extraction.
    """

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.LOCAL_FS
            and self.destination_type == UrlType.LOCAL_FS
        ):
            raise ValueError(
                f"Can only copy from {UrlType.LOCAL_FS.name} "
                f"to {UrlType.LOCAL_FS.name}"
            )

    async def _recompress(self) -> str:
        self._ensure_recompression_possible()
        if self._can_skip_recompression():
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_extraction_destination = self.temp_dir / "extracted"
        extracted_folder = await extract(
            source=Path(self.source), destination=temp_extraction_destination
        )
        new_archive = await compress(
            source=extracted_folder, destination=Path(self.destination)
        )
        return str(new_archive)

    async def _extract(self) -> str:
        if self.source_filename is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        extracted_folder = await extract(
            source=Path(self.source), destination=Path(self.destination)
        )
        return str(extracted_folder)

    async def _compress(self) -> str:
        if self.destination_filename is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        compressed_file = await compress(
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

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.LOCAL_FS
            and self.destination_type == UrlType.CLOUD
        ):
            raise ValueError(
                f"Can only copy from {UrlType.LOCAL_FS.name} "
                f"to {UrlType.CLOUD.name}"
            )

    async def _recompress_and_copy(self) -> str:
        self._ensure_recompression_possible()
        if self._can_skip_recompression():
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_archive = (
            self.temp_dir / "recompressed" / self.destination_filename  # type: ignore
        )
        local_copier = LocalToLocalCopier(
            source=self.source,
            destination=str(temp_archive),
            extract=True,
            compress=True,
            temp_dir=self.temp_dir,
        )
        logger.debug(f"Using {local_copier} to perform recompression locally")
        copy_source = await local_copier.perform_copy()
        copier_implementation = BaseLocalCopier.get_copier(
            source=copy_source, destination=self.destination, type=self.destination_type
        )
        return await copier_implementation.perform_copy()

    async def _extract_and_copy(self) -> str:
        if self.source_filename is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        extracted_folder = await extract(
            source=Path(self.source), destination=self.temp_dir
        )
        copy_source = str(extracted_folder)
        copier_implementation = BaseLocalCopier.get_copier(
            source=copy_source, destination=self.destination, type=self.destination_type
        )
        return await copier_implementation.perform_copy()

    async def _compress_and_copy(self) -> str:
        if self.destination_filename is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        compressed_file = await compress(
            source=Path(self.source),
            destination=self.temp_dir / self.destination_filename,
        )
        copy_source = str(compressed_file)
        copier_implementation = BaseLocalCopier.get_copier(
            source=copy_source, destination=self.destination, type=self.destination_type
        )
        return await copier_implementation.perform_copy()

    async def _copy(self) -> str:
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source, destination=self.destination, type=self.destination_type
        )
        return await copier_implementation.perform_copy()

    async def perform_copy(self) -> str:
        """Perform copy from local to cloud.

        Delegates copy implementation to appropriate Copier (S3, Azure, GCS, Web).
        Uses ArchiveManager to handle compression/extraction.
        """
        if self.extract and self.compress:
            return await self._recompress_and_copy()
        elif self.extract:
            return await self._extract_and_copy()
        elif self.compress:
            return await self._compress_and_copy()
        else:
            return await self._copy()


class CloudToLocalCopier(BaseLocalCopier):
    """Copier, that can copy data from cloud storage to local fs

    Supports compression and extraction (temp_dir is used to store intermediate results)
    """

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.CLOUD
            and self.destination_type == UrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {UrlType.CLOUD.name} "
                f"and {UrlType.LOCAL_FS.name}"
            )

    async def _copy_and_recompress(self) -> str:
        self._ensure_recompression_possible()
        if self._can_skip_recompression():
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_source_archive = self.temp_dir / self.source_filename  # type: ignore
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=str(temp_source_archive),
            type=self.source_type,
        )
        temp_location = await copier_implementation.perform_copy()
        local_copier = LocalToLocalCopier(
            source=temp_location,
            destination=self.destination,
            extract=True,
            compress=True,
            temp_dir=self.temp_dir,
        )
        logger.debug(f"Using {local_copier} to perform recompression locally")
        return await local_copier.perform_copy()

    async def _copy_and_extract(self) -> str:
        if self.source_filename is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        temp_archive = str(self.temp_dir / self.source_filename)
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source, destination=temp_archive, type=self.source_type
        )
        archive = await copier_implementation.perform_copy()
        extraction_result = await extract(
            source=Path(archive), destination=Path(self.destination)
        )
        return str(extraction_result)

    async def _copy_and_compress(self) -> str:
        if self.destination_filename is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        if self.source_filename:
            destination_path = self.temp_dir / self.source_filename
        else:
            destination_path = self.temp_dir / "source"
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=str(destination_path),
            type=self.source_type,
        )
        directory = await copier_implementation.perform_copy()
        compression_result = await compress(
            source=Path(directory), destination=Path(self.destination)
        )
        return str(compression_result)

    async def _copy(self) -> str:
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source, destination=self.destination, type=self.source_type
        )
        return await copier_implementation.perform_copy()

    async def perform_copy(self) -> str:
        if self.extract and self.compress:
            return await self._copy_and_recompress()
        elif self.extract:
            return await self._copy_and_extract()
        elif self.compress:
            return await self._copy_and_compress()
        else:
            return await self._copy()
