"""Module for common functionality and key abstractions related to data copy"""
import abc
import logging
import os
import re
from dataclasses import dataclass
from enum import Flag, auto
from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Tuple, Union

from neuro_sdk import Client
from yarl import URL


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
    GZ = auto()
    ZIP = auto()
    TAR = TAR_PLAIN | TAR_GZ | TAR_BZ
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
            ".bz2": ArchiveType.TAR_BZ,
            ".tbz": ArchiveType.TAR_BZ,
            ".tar": ArchiveType.TAR_PLAIN,
            ".gz": ArchiveType.GZ,
            ".zip": ArchiveType.ZIP,
        }

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ArchiveType):
            return bool(int(self) & int(other))
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


class DataUrlType(int, Flag):  # type: ignore
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
    COPY_UNSUPPORTED = ~(LOCAL_FS | CLOUD | PLATFORM)
    COPY_SUPPORTED = ~COPY_UNSUPPORTED

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataUrlType):
            return bool(self & other)
        return False

    def __hash__(self) -> int:
        return hash(int(self))

    @staticmethod
    def get_scheme_mapping() -> Dict[str, "DataUrlType"]:
        """Get supported mapping of url schema to UrlType"""
        return {
            "": DataUrlType.LOCAL_FS,
            "s3": DataUrlType.S3,
            "gs": DataUrlType.GCS,
            "azure+https": DataUrlType.AZURE,
            "storage": DataUrlType.STORAGE,
            "disk": DataUrlType.DISK,
            "http": DataUrlType.HTTP,
            "https": DataUrlType.HTTPS,
        }

    @staticmethod
    def get_type(url: Union[str, URL]) -> "DataUrlType":
        """Detect UrlType by checking url schema"""
        scheme_mapping = DataUrlType.get_scheme_mapping()
        if isinstance(url, URL):
            url_scheme = url.scheme
        else:
            url_scheme = URL(url).scheme
        return scheme_mapping.get(url_scheme, DataUrlType.COPY_UNSUPPORTED)


class Copier(metaclass=abc.ABCMeta):
    """Base interface for copying data between a variety of sources"""

    def __init__(self, source: "Resource", destination: "Resource") -> None:
        self.source = source
        self.destination = destination
        self._ensure_can_execute()

    def _ensure_can_execute(self) -> None:
        """Raise error if copy cannot be executed"""
        raise NotImplementedError

    async def perform_copy(self) -> "Resource":
        """Copy data from self.source to self.destination
        and return path to the copied resource"""
        raise NotImplementedError


@dataclass(frozen=True)
class Resource:
    """Represents a resource, pointed to by a URL"""

    url: URL
    _client: Optional[Client] = None  # only present for platform resources

    @cached_property
    def data_copy_supported(self) -> bool:
        result = self.data_url_type == DataUrlType.COPY_SUPPORTED
        logger.debug(f"Data copy supported for {self.url}: {result}")
        return result

    @cached_property
    def data_url_type(self) -> DataUrlType:
        result = DataUrlType.get_type(self.url)
        logger.debug(f"Data url type of {self.url}: {result.name}")
        return result

    @cached_property
    def archive_type(self) -> ArchiveType:
        result = ArchiveType.get_type(archive=Path(self.url.path))
        logger.debug(f"Archive type of {self.url}: {result.name}")
        return result

    @cached_property
    def filename(self) -> Optional[str]:
        """Filename to which the url is pointing

        For local urls, this is obtained from os.path.split
        For platform urls this is obtained by detecting cluster/org/project name,
          if the url points to a project/org/cluster (without trailing slash),
          the filename will be None.
        For all other urls - the last part of self.url.parts is returned
        """
        if self.data_url_type == DataUrlType.LOCAL_FS:
            # use pathlib
            _, tail = os.path.split(self.as_path())
            result = tail if tail else None

        elif self.data_url_type in (DataUrlType.DISK, DataUrlType.STORAGE):
            assert self._client is not None
            normalized_url = self._client.parse.normalize_uri(
                self.strip_mount_mode_flag().url
            )
            parts = normalized_url.parts
            # during normaliziation at least current project is present
            assert len(parts) >= 2
            cluster_name = normalized_url.host
            assert cluster_name is not None
            if parts[1] in self._client.config.cluster_orgs:
                # url of type {schema}://{cluster_name}/{org}/{project}[rest]
                logger.debug(f"{self.url} (normalized: {normalized_url}) has org")
                assert len(parts) >= 3
                parts_to_skip = 3
            else:
                # url of type {schema}://{cluster_name}/{project}[rest]
                logger.debug(f"{self.url} (normalized: {normalized_url}) has no org")
                parts_to_skip = 2
            if self.data_url_type == DataUrlType.DISK:
                # url of type
                # {schema}://{cluster_name}[/{org}]/{project}/{disk-id}[rest]
                parts_to_skip += 1
            parts = parts[parts_to_skip:]
            if parts:
                result = parts[-1] if parts[-1] else None
            else:
                result = None
        else:
            parts = self.url.parts
            if parts:
                result = parts[-1] if parts[-1] else None
            else:
                result = None
        logger.debug(f"Filename of {self.url} ({self.data_url_type.name}): {result}")
        return result

    @cached_property
    def disk_id_and_path(self) -> Tuple[str, Optional[str]]:
        """For disk resources - full id of the disk
        (disk://{cluster}[/org]/project/id) and the path on disk

        Fails if self._client is None.
        """
        assert self.data_url_type == DataUrlType.DISK
        assert self._client is not None
        normalized_url = self._client.parse.normalize_uri(
            self.strip_mount_mode_flag().url
        )
        parts = normalized_url.parts
        # during normaliziation at least current project and disk id is present
        assert len(parts) >= 3
        cluster_name = normalized_url.host
        assert cluster_name is not None
        if parts[1] in self._client.config.cluster_orgs:
            # url of type {schema}://{cluster_name}/{org}/{project}[rest]
            logger.debug(f"{self.url} (normalized: {normalized_url}) has org")
            assert len(parts) >= 3
            disk_name_len = 4
        else:
            logger.debug(f"{self.url} (normalized: {normalized_url}) has no org")
            disk_name_len = 3
        org_project_name = "/".join(parts[1:disk_name_len])
        full_disk_id = f"disk://{cluster_name}/{org_project_name}"
        if parts[disk_name_len:]:
            path_on_disk = "/" + "/".join(parts[disk_name_len:])
        else:
            path_on_disk = None
        logger.debug(f"{self.url} {full_disk_id=} {path_on_disk=}")
        return full_disk_id, path_on_disk

    @staticmethod
    def parse(url: str, client: Client) -> "Resource":
        """Parse resource spec, normalizing platform urls if needed"""
        parsed_url = URL(url)
        scheme = parsed_url.scheme
        if scheme in ("storage", "disk"):
            return Resource(
                url=client.parse.normalize_uri(parsed_url),
                _client=client,
            )
        return Resource(url=parsed_url)

    @staticmethod
    def from_path(path: Path) -> "Resource":
        return Resource(URL(str(path)))

    @staticmethod
    def from_str(url: str, *, _client: Optional[Client] = None) -> "Resource":
        """Create a resource from the string url, without normalization"""
        return Resource(URL(url), _client=_client)

    def as_path(self) -> Path:
        return Path(self.url.path)

    def as_str(self) -> str:
        return str(self.url)

    def __str__(self) -> str:
        return self.as_str()

    def strip_filename(self) -> "Resource":
        """
        Return a Resource pointing to the parent folder
        if the url points to a file
        """
        if self.filename is None:
            logger.debug(f"{self.url} has no fileneme")
            return self
        result = Resource.from_str(
            re.sub(pattern=f"{self.filename}$", repl="", string=self.as_str()),
            _client=self._client,
        )
        logger.debug(f"Stripped filename from {self.url} to {result.url}")
        return result

    @cached_property
    def mode_flag(self) -> Optional[str]:
        """Get :ro or :rw flag from platform urls"""
        if self.data_url_type not in (DataUrlType.DISK, DataUrlType.STORAGE):
            logger.debug(f"{self.url} ({self.data_url_type=}) has no mode flag.")
            return None
        match = re.search(pattern=f":(rw|ro)$", string=self.as_str())
        if match:
            result = match.group(1)
            logger.debug(f"Mode flag of {self.url}: {result}")
            return result
        logger.debug(f"{self.url} ({self.data_url_type=}) has no mode flag.")
        return None

    def strip_mount_mode_flag(self) -> "Resource":
        if self.data_url_type in (DataUrlType.DISK, DataUrlType.STORAGE):
            return Resource.from_str(
                re.sub(pattern="(:ro|:rw)$", repl="", string=self.as_str()),
                _client=self._client,
            )
        return self


def ensure_folder_exists(local_resource: Resource) -> None:
    """Ensure the folder (in case of the file resource - parent folder) exists"""
    folder_name, _ = os.path.split(local_resource.as_str())
    logger.info(f"Creating folder for {folder_name}")
    Path(folder_name).mkdir(exist_ok=True, parents=True)


def get_default_preset(neuro_client: Client) -> str:
    """Get default preset name via Neu.ro client"""
    return next(iter(neuro_client.presets.keys()))


def provide_temp_dir(
    dir: Path = Path.home() / ".neuro-tmp",
) -> TemporaryDirectory:  # type: ignore
    """Provide temp directory"""
    dir.mkdir(exist_ok=True, parents=True)
    return TemporaryDirectory(dir=dir)
