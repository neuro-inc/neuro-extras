"""Module for handling copy operations, that can be run locally

Contains:
- LocalToLocalCopier
- LocalToCloudCopier
- CloudToLocalCopier
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Type

from apolo_extras.data.fs import LocalFSCopier
from apolo_extras.data.web import WebCopier

from .archive import ArchiveType, compress, extract
from .azure import AzureCopier
from .common import Copier, DataUrlType, Resource, ensure_folder_exists
from .gcs import GCSCopier
from .s3 import S3Copier


logger = logging.getLogger(__name__)


class BaseLocalCopier(Copier):
    """Base class for copiers, which can be executed locally"""

    def __init__(
        self,
        source: Resource,
        destination: Resource,
        compress: bool = False,
        extract: bool = False,
        temp_dir: Path = Path(tempfile.gettempdir()),
    ) -> None:
        super().__init__(source=source, destination=destination)
        self.compress = compress
        self.extract = extract
        self.temp_dir = temp_dir

    @staticmethod
    def get_copier(
        source: Resource, destination: Resource, type: DataUrlType
    ) -> Copier:
        """Get copier of proper type to copy from"""
        destination_copier_mapping = {
            DataUrlType.S3: S3Copier,
            DataUrlType.AZURE: AzureCopier,
            DataUrlType.GCS: GCSCopier,
            DataUrlType.HTTP: WebCopier,
            DataUrlType.HTTPS: WebCopier,
            DataUrlType.LOCAL_FS: LocalFSCopier,
        }
        cls: Type[Copier] = destination_copier_mapping[type]
        return cls(source=source, destination=destination)

    def _can_skip_recompression(self) -> bool:
        """Check if both urls point to archives of same type"""
        if None in (self.source.filename, self.destination.filename):
            # at least one is a directory
            return False
        if ArchiveType.UNSUPPORTED in (
            self.source.archive_type,
            self.destination.archive_type,
        ):
            # at least one is not a supported archive
            return False
        return self.source.archive_type == self.destination.archive_type

    def _ensure_recompression_possible(self) -> None:
        """Raise error if recompression is not possible"""
        if None in (self.source.filename, self.destination.filename):
            # at least one is a directory
            raise ValueError("Recompression is unsupported for directories")
        if ArchiveType.UNSUPPORTED in (
            self.source.archive_type,
            self.destination.archive_type,
        ):
            raise ValueError("Recompression is not possible for unsupported archives")


class LocalToLocalCopier(BaseLocalCopier):
    """Copier, that can copy data from and to local storage

    Supports compression and extraction.
    """

    def _ensure_can_execute(self) -> None:
        if not (
            self.source.data_url_type == DataUrlType.LOCAL_FS
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError(
                f"Can only copy from {DataUrlType.LOCAL_FS.name} "
                f"to {DataUrlType.LOCAL_FS.name}"
            )

    async def _recompress(self) -> Resource:
        self._ensure_recompression_possible()
        if self._can_skip_recompression():
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_extraction_destination = self.temp_dir / "extracted"
        extracted_folder = await extract(
            source=self.source,
            destination=Resource.from_path(temp_extraction_destination),
        )

        new_archive = await compress(
            source=extracted_folder, destination=self.destination
        )
        return new_archive

    async def _extract(self) -> Resource:
        if self.source.filename is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        extracted_folder = await extract(
            source=self.source,
            destination=self.destination,
        )
        return extracted_folder

    async def _compress(self) -> Resource:
        if self.destination.filename is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        compressed_file = await compress(
            source=self.source,
            destination=self.destination,
        )
        return compressed_file

    async def _copy(self) -> Resource:
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source, destination=self.destination, type=DataUrlType.LOCAL_FS
        )
        return await copier_implementation.perform_copy()

    async def perform_copy(self) -> Resource:
        """Perform copy from local fs to local fs.

        Delegates copy implementation to appropriate LocalFSCopier.
        Uses ArchiveManager to handle compression/extraction.
        """
        ensure_folder_exists(self.destination)
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
            self.source.data_url_type == DataUrlType.LOCAL_FS
            and self.destination.data_url_type == DataUrlType.CLOUD
        ):
            raise ValueError(
                f"Can only copy from {DataUrlType.LOCAL_FS.name} "
                f"to {DataUrlType.CLOUD.name}"
            )

    async def _recompress_and_copy(self) -> Resource:
        self._ensure_recompression_possible()
        if self._can_skip_recompression():
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_archive = (
            self.temp_dir / "recompressed" / self.destination.filename  # type: ignore
        )
        local_copier = LocalToLocalCopier(
            source=self.source,
            destination=Resource.from_path(temp_archive),
            extract=True,
            compress=True,
            temp_dir=self.temp_dir,
        )
        logger.debug(f"Using {local_copier} to perform recompression locally")
        copy_source = await local_copier.perform_copy()
        copier_implementation = BaseLocalCopier.get_copier(
            source=copy_source,
            destination=self.destination,
            type=self.destination.data_url_type,
        )
        return await copier_implementation.perform_copy()

    async def _extract_and_copy(self) -> Resource:
        if self.source.filename is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        extracted_folder = await extract(
            source=self.source, destination=Resource.from_path(self.temp_dir)
        )
        copy_source = str(extracted_folder) + os.sep
        copier_implementation = BaseLocalCopier.get_copier(
            source=Resource.from_str(copy_source),
            destination=self.destination,
            type=self.destination.data_url_type,
        )
        return await copier_implementation.perform_copy()

    async def _compress_and_copy(self) -> Resource:
        if self.destination.filename is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        compressed_file = await compress(
            source=self.source,
            destination=Resource.from_path(self.temp_dir / self.destination.filename),
        )
        copier_implementation = BaseLocalCopier.get_copier(
            source=compressed_file,
            destination=self.destination,
            type=self.destination.data_url_type,
        )
        return await copier_implementation.perform_copy()

    async def _copy(self) -> Resource:
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=self.destination,
            type=self.destination.data_url_type,
        )
        return await copier_implementation.perform_copy()

    async def perform_copy(self) -> Resource:
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
            self.source.data_url_type == DataUrlType.CLOUD
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {DataUrlType.CLOUD.name} "
                f"and {DataUrlType.LOCAL_FS.name}"
            )

    async def _copy_and_recompress(self) -> Resource:
        self._ensure_recompression_possible()
        if self._can_skip_recompression():
            logger.info(
                "Skipping compression step - "
                "source and destination are archives of the same type"
            )
            return await self._copy()
        temp_source_archive: Path = self.temp_dir / self.source.filename  # type: ignore
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=Resource.from_path(temp_source_archive),
            type=self.source.data_url_type,
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

    async def _copy_and_extract(self) -> Resource:
        if self.source.filename is None:
            raise ValueError(f"Can't infer archive type from source {self.source}")
        temp_archive = self.temp_dir / self.source.filename
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=Resource.from_path(temp_archive),
            type=self.source.data_url_type,
        )
        archive = await copier_implementation.perform_copy()
        extraction_result = await extract(source=archive, destination=self.destination)
        return extraction_result

    async def _copy_and_compress(self) -> Resource:
        if self.destination.filename is None:
            raise ValueError(
                f"Can't infer archive type from destination {self.destination}"
            )
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        if self.source.filename:
            destination_path = str(self.temp_dir / self.source.filename)
        else:
            (self.temp_dir / "source").mkdir(exist_ok=True, parents=True)
            destination_path = str(self.temp_dir / "source") + os.sep
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=Resource.from_str(destination_path),
            type=self.source.data_url_type,
        )
        directory = await copier_implementation.perform_copy()
        compression_result = await compress(
            source=directory, destination=self.destination
        )
        return compression_result

    async def _copy(self) -> Resource:
        copier_implementation = BaseLocalCopier.get_copier(
            source=self.source,
            destination=self.destination,
            type=self.source.data_url_type,
        )
        return await copier_implementation.perform_copy()

    async def perform_copy(self) -> Resource:
        ensure_folder_exists(self.destination)
        if self.extract and self.compress:
            return await self._copy_and_recompress()
        elif self.extract:
            return await self._copy_and_extract()
        elif self.compress:
            return await self._copy_and_compress()
        else:
            return await self._copy()
