"""Module for data operations

Provides:
- CopyOperation
"""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

from apolo_sdk import Client

from ..utils import provide_temp_dir
from .common import Copier, DataUrlType, Resource
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
        client: Client,
        volumes: Optional[List[str]] = None,
        env: Optional[List[str]] = None,
        life_span: Optional[float] = None,
        preset: Optional[str] = None,
    ) -> None:
        self.client = client
        self.source = Resource.parse(source, client=client)
        self.destination = Resource.parse(destination, client=client)
        self.compress = compress
        self.extract = extract
        self.volumes = volumes
        self.env = env
        self.life_span = life_span
        self.preset = preset
        self._ensure_can_execute()

    def _ensure_can_execute(self) -> None:
        """Raise exception if operation is unsupported

        Possible reasons:
        - at least one of the source or destination is of unsupported type
        - pair (source, destination) is explicitly forbidden
        """
        if not self.source.data_copy_supported:
            raise ValueError(f"Unsupported source: {self.source}")
        if not self.destination.data_copy_supported:
            raise ValueError(f"Unsupported destination: {self.destination}")
        source_type = DataUrlType.get_type(self.source.url)
        destination_type = DataUrlType.get_type(self.destination.url)
        is_forbidden_combination = any(
            (
                self.source.data_url_type == source
                and self.destination.data_url_type == destination
            )
            for (source, destination) in CopyOperation.get_forbidden_combinations()
        )
        if is_forbidden_combination:
            raise ValueError(
                f"Copy from {source_type.name} to "
                f"{destination_type.name} is unsupported. "
                "Please, reach us at https://github.com/neuro-inc/neuro-extras/issues "
                "describing your use case."
            )
        else:
            logger.debug(
                f"Copy from {source_type.name} to "
                f"{destination_type.name} is supported"
            )

    async def run(self) -> None:
        """Run copy operation.

        Uses appropriate copier instance, that supports source and destionation
        """

        with provide_temp_dir() as temp_dir:
            logger.debug("Resolving copier...")
            copier = _get_copier(
                source=self.source,
                destination=self.destination,
                compress=self.compress,
                extract=self.extract,
                temp_dir=Path(temp_dir),
                client=self.client,
                volumes=self.volumes,
                env=self.env,
                life_span=self.life_span,
                preset=self.preset,
            )
            logger.debug(f"Using {copier.__class__.__name__}")
            await copier.perform_copy()

    @staticmethod
    def get_forbidden_combinations() -> List[Tuple[DataUrlType, DataUrlType]]:
        """Get forbidden combinations of source and destination types"""
        return [
            (DataUrlType.CLOUD, DataUrlType.CLOUD),
            # TODO: (A.K.) implement platform-to-local and vice-versa
            # through apolo storage cp
            (DataUrlType.PLATFORM, DataUrlType.LOCAL_FS),
            (DataUrlType.LOCAL_FS, DataUrlType.PLATFORM),
            (DataUrlType.STORAGE, DataUrlType.STORAGE),
            (DataUrlType.DISK, DataUrlType.DISK),
            (DataUrlType.COPY_SUPPORTED, DataUrlType.WEB),
        ]


def _get_copier(
    source: Resource,
    destination: Resource,
    compress: bool,
    extract: bool,
    temp_dir: Path,
    client: Client,
    volumes: Optional[List[str]] = None,
    env: Optional[List[str]] = None,
    preset: Optional[str] = None,
    life_span: Optional[float] = None,
) -> Copier:
    """Resolve an instance of Copier, which is able to copy
    from source to destination with provided params"""
    source_type = DataUrlType.get_type(source.url)
    destination_type = DataUrlType.get_type(destination.url)
    if source_type == DataUrlType.LOCAL_FS and destination_type == DataUrlType.CLOUD:
        return LocalToCloudCopier(
            source=source,
            destination=destination,
            compress=compress,
            extract=extract,
            temp_dir=temp_dir,
        )
    elif source_type == DataUrlType.CLOUD and destination_type == DataUrlType.LOCAL_FS:
        return CloudToLocalCopier(
            source=source,
            destination=destination,
            compress=compress,
            extract=extract,
            temp_dir=temp_dir,
        )
    elif (
        source_type == DataUrlType.CLOUD
        and destination_type == DataUrlType.PLATFORM
        or source_type == DataUrlType.PLATFORM
        and destination_type == DataUrlType.CLOUD
        or source_type == DataUrlType.PLATFORM
        and destination_type == DataUrlType.PLATFORM
    ):
        return RemoteCopier(
            source=source,
            destination=destination,
            client=client,
            compress=compress,
            extract=extract,
            volumes=volumes,
            preset=preset,
            env=env,
            life_span=life_span,
        )
    elif source_type == DataUrlType.LOCAL_FS and destination_type.LOCAL_FS:
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
