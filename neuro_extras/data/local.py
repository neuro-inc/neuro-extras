import logging
import tempfile
from pathlib import Path
from typing import Type

from .archive import ArchiveManager
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
        destination_copier_mapping = {
            UrlType.S3: S3Copier,
            UrlType.AZURE: AzureCopier,
            UrlType.GCS: GCSCopier,
        }
        cls: Type[Copier] = destination_copier_mapping[type]
        return cls(source=source, destination=destination)


class LocalToCloudCopier(BaseLocalCopier):
    """Copier, that can copy data from local storage to a storage bucket

    Supports compression and extraction (temp_dir is used to store intermediate results)
    """

    async def perform_copy(self) -> str:

        if self.compress:
            archive_name = get_filename_from_url(self.destination)
            if archive_name is None:
                raise ValueError(
                    f"Can't infer archive type from destination {self.destination}"
                )
            compressed_file = await self.archive_manager.compress(
                source=Path(self.source), destination=self.temp_dir / archive_name
            )
            copy_source = str(compressed_file)
        elif self.extract:
            extracted_folder = await self.archive_manager.extract(
                source=Path(self.source), destination=self.temp_dir
            )
            copy_source = str(extracted_folder)
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

    async def perform_copy(self) -> str:
        if self.compress:
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
        elif self.extract:
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
        else:
            copier_implementation = BaseLocalCopier.get_copier(
                source=self.source, destination=self.destination, type=self.source_type
            )
            return await copier_implementation.perform_copy()
