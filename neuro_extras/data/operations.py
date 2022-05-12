"""Module for data operations

Provides:
- CopyOperation
"""
import logging
from pathlib import Path
from typing import List, Optional, Tuple

from neuro_sdk import Client

from ..utils import get_neuro_client, provide_temp_dir
from .common import Copier, UrlType, get_filename_from_url
from .local import CloudToLocalCopier, LocalToCloudCopier, LocalToLocalCopier
from .remote import RemoteCopier


logger = logging.getLogger(__name__)

# TODO: (A.K.) implement TransferOperation


class CopyOperation:
    """Abstraction of data copying between two locations
    with support for compression and extraction"""

    def __init__(
        self,
        source: str,
        destination: str,
        compress: bool,
        extract: bool,
        volumes: Optional[List[str]] = None,
        env: Optional[List[str]] = None,
        life_span: Optional[float] = None,
        preset: Optional[str] = None,
    ) -> None:
        self.source = source
        self.destination = destination
        self.compress = compress
        self.extract = extract
        self.volumes = volumes
        self.env = env
        self.life_span = life_span
        self.preset = preset
        self.source_type = UrlType.get_type(source)
        self.destination_type = UrlType.get_type(destination)
        self.source_filename = get_filename_from_url(source)
        self.destination_filename = get_filename_from_url(destination)
        self._ensure_can_execute()

    def _ensure_can_execute(self) -> None:
        """Raise exception if operation is unsupported

        Possible reasons:
        - at least one of the source or destination is of unsupported type
        - pair (source, destination) is explicitly forbidden
        """
        if self.source_type == UrlType.UNSUPPORTED:
            raise ValueError(f"Unsupported source: {self.source}")
        if self.destination_type == UrlType.UNSUPPORTED:
            raise ValueError(f"Unsupported destination: {self.destination}")
        is_forbidden_combination = any(
            self.source_type == source and self.destination_type == destination
            for (source, destination) in CopyOperation.get_forbidden_combinations()
        )
        if is_forbidden_combination:
            raise ValueError(
                f"Copy from {self.source_type.name} to "
                f"{self.destination_type.name} is unsupported. "
                "Please, reach us at https://github.com/neuro-inc/neuro-extras/issues "
                "describing your use case."
            )
        else:
            logger.debug(
                f"Copy from {self.source_type.name} to "
                f"{self.destination_type.name} is supported"
            )

    async def run(self) -> None:
        """Run copy operation.

        Uses appropriate copier instance, that supports source and destionation
        """
        async with get_neuro_client() as neuro_client:
            with provide_temp_dir() as temp_dir:
                logger.debug("Resolving copier...")
                copier = _get_copier(
                    source=self.source,
                    destination=self.destination,
                    compress=self.compress,
                    extract=self.extract,
                    temp_dir=Path(temp_dir),
                    neuro_client=neuro_client,
                    volumes=self.volumes,
                    env=self.env,
                    life_span=self.life_span,
                    preset=self.preset,
                )
                logger.debug(f"Using {copier.__class__.__name__}")
                await copier.perform_copy()

    @staticmethod
    def get_forbidden_combinations() -> List[Tuple[UrlType, UrlType]]:
        """Get forbidden combinations of source and destination types"""
        return [
            (UrlType.CLOUD, UrlType.CLOUD),
            # TODO: (A.K.) implement platform-to-local and vice-versa
            # through neuro storage cp
            (UrlType.PLATFORM, UrlType.LOCAL_FS),
            (UrlType.LOCAL_FS, UrlType.PLATFORM),
            (UrlType.STORAGE, UrlType.STORAGE),
            (UrlType.DISK, UrlType.DISK),
            (UrlType.SUPPORTED, UrlType.WEB),
        ]


def _get_copier(
    source: str,
    destination: str,
    compress: bool,
    extract: bool,
    temp_dir: Path,
    neuro_client: Client,
    volumes: Optional[List[str]] = None,
    env: Optional[List[str]] = None,
    preset: Optional[str] = None,
    life_span: Optional[float] = None,
) -> Copier:
    """Resolve an instance of Copier, which is able to copy
    from source to destination with provided params"""
    source_type = UrlType.get_type(source)
    destination_type = UrlType.get_type(destination)
    if source_type == UrlType.LOCAL_FS and destination_type == UrlType.CLOUD:
        return LocalToCloudCopier(
            source=source,
            destination=destination,
            compress=compress,
            extract=extract,
            temp_dir=temp_dir,
        )
    elif source_type == UrlType.CLOUD and destination_type == UrlType.LOCAL_FS:
        return CloudToLocalCopier(
            source=source,
            destination=destination,
            compress=compress,
            extract=extract,
            temp_dir=temp_dir,
        )
    elif (
        source_type == UrlType.CLOUD
        and destination_type == UrlType.PLATFORM
        or source_type == UrlType.PLATFORM
        and destination_type == UrlType.CLOUD
        or source_type == UrlType.PLATFORM
        and destination_type == UrlType.PLATFORM
    ):
        return RemoteCopier(
            source=source,
            destination=destination,
            neuro_client=neuro_client,
            compress=compress,
            extract=extract,
            volumes=volumes,
            preset=preset,
            env=env,
            life_span=life_span,
        )
    elif source_type == UrlType.LOCAL_FS and destination_type.LOCAL_FS:
        return LocalToLocalCopier(
            source=source,
            destination=destination,
            compress=compress,
            extract=extract,
            temp_dir=temp_dir,
        )
    else:
        raise NotImplementedError(
            f"No copier found, that supports copy "
            f"from {source_type.name} to {destination_type.name}"
        )
