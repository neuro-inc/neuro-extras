import logging
from dataclasses import dataclass
from typing import List, Mapping, Optional, Tuple

from neuro_sdk import Client, DiskVolume, RemoteImage, SecretFile, Volume
from yarl import URL

from ..common import EX_OK, NEURO_EXTRAS_IMAGE, _attach_job_stdout
from .common import Copier, UrlType, get_filename_from_url
from .utils import get_default_preset


logger = logging.getLogger(__name__)


@dataclass
class RemoteJobConfig:
    """Arguments, passed to `neuro_sdk.Client.jobs.start()`"""

    image: RemoteImage
    command: str
    env: Optional[Mapping[str, str]]
    secret_env: Optional[Mapping[str, URL]]
    volumes: List[Volume]
    secret_files: List[SecretFile]
    disk_volumes: List[DiskVolume]
    preset_name: str
    life_span: Optional[float]


class RemoteCopier(Copier):
    """Copier, that creates a job on neu.ro platform.
    Can copy data between neu.ro storage: or disk: and cloud storage"""

    def __init__(
        self,
        source: str,
        destination: str,
        neuro_client: Client,
        compress: bool = False,
        extract: bool = False,
        volumes: Optional[List[str]] = None,
        env: Optional[List[str]] = None,
        preset: Optional[str] = None,
        life_span: Optional[float] = None,
    ) -> None:
        super().__init__(source, destination)
        self.neuro_client = neuro_client
        self.job_config = RemoteCopier.create_remote_job_config(
            source=source,
            destination=destination,
            neuro_client=neuro_client,
            compress=compress,
            extract=extract,
            volumes=volumes,
            env=env,
            preset=preset,
            life_span=life_span,
        )

    @staticmethod
    def map_into_volumes(
        source: str,
        destination: str,
        source_storage_mount_prefix: str = "/var/storage/source",
        destination_storage_mount_prefix: str = "/var/storage/destination",
        source_disk_mount_prefix: str = "/var/disk/source",
        destination_disk_mount_prefix: str = "/var/disk/destination",
    ) -> Tuple[str, str, List[str]]:
        """Map urls for platform storage into volume mounts

        Returns (patched_source: str, patched_destination: str, volumes: List[str]),
        where patched_source and patched_destination are mount points for
        source and destination if they belong to platform storage
        and the same urls otherwise
        """

        def map_singe_url_into_volumes(
            url: str, storage_mount_prefix: str, disk_mount_prefix: str
        ) -> Tuple[str, List[str]]:
            volumes = []
            url_type = UrlType.get_type(url)
            filename = get_filename_from_url(url)
            if url_type == UrlType.STORAGE:
                if filename:
                    new_url = f"{storage_mount_prefix}/{filename}"
                else:
                    new_url = f"{storage_mount_prefix}/"
                volumes.append(f"{url}:{new_url}")
            elif url_type == UrlType.DISK:
                if filename:
                    new_url = f"{disk_mount_prefix}/{filename}"
                else:
                    new_url = f"{disk_mount_prefix}/"
                volumes.append(f"{url}:{new_url}")
            else:
                new_url = url
            return new_url, volumes

        new_source, source_mounts = map_singe_url_into_volumes(
            url=source,
            storage_mount_prefix=source_storage_mount_prefix,
            disk_mount_prefix=source_disk_mount_prefix,
        )

        new_destination, destination_mounts = map_singe_url_into_volumes(
            url=destination,
            storage_mount_prefix=destination_storage_mount_prefix,
            disk_mount_prefix=destination_disk_mount_prefix,
        )

        return new_source, new_destination, source_mounts + destination_mounts

    @staticmethod
    def create_remote_job_config(
        source: str,
        destination: str,
        neuro_client: Client,
        compress: bool = False,
        extract: bool = False,
        volumes: Optional[List[str]] = None,
        env: Optional[List[str]] = None,
        preset: Optional[str] = None,
        life_span: Optional[float] = None,
    ) -> RemoteJobConfig:
        """Create `RemoteJobConfig` for a neu.ro copy job.

        Copy job will copy data from `source` to `destination`"""
        image = neuro_client.parse.remote_image(NEURO_EXTRAS_IMAGE)

        (
            patched_source,
            patched_destination,
            data_mounts,
        ) = RemoteCopier.map_into_volumes(
            source=source,
            destination=destination,
        )
        command = RemoteCopier.build_command(
            source=patched_source,
            destination=patched_destination,
            extract=extract,
            compress=compress,
        )
        all_volumes = volumes + data_mounts if volumes else data_mounts
        env_parse_result = neuro_client.parse.envs(env if env else [])
        volume_parse_result = neuro_client.parse.volumes(all_volumes)
        preset_name = preset or get_default_preset(neuro_client)
        return RemoteJobConfig(
            image=image,
            command=command,
            env=env_parse_result.env,
            secret_env=env_parse_result.secret_env,
            volumes=list(volume_parse_result.volumes),
            disk_volumes=list(volume_parse_result.disk_volumes),
            secret_files=list(volume_parse_result.secret_files),
            preset_name=preset_name,
            life_span=life_span,
        )

    @staticmethod
    def build_command(
        source: str, destination: str, extract: bool, compress: bool
    ) -> str:
        command_prefix = ["neuro-extras", "data", "cp"]
        args = [source, destination]
        flags = []
        if compress:
            flags.append("-c")
        if extract:
            flags.append("-x")
        full_command = command_prefix + flags + args
        return " ".join(full_command)

    async def perform_copy(self) -> str:
        logger.info(f"Starting job from config: {self.job_config}")
        job = await self.neuro_client.jobs.start(
            image=self.job_config.image,
            command=self.job_config.command,
            env=self.job_config.env,
            secret_env=self.job_config.secret_env,
            volumes=self.job_config.volumes,
            secret_files=self.job_config.secret_files,
            disk_volumes=self.job_config.disk_volumes,
            preset_name=self.job_config.preset_name,
            life_span=self.job_config.life_span,
        )
        exit_code = await _attach_job_stdout(job, self.neuro_client, name="copy")
        if exit_code == EX_OK:
            logger.info("Copy job finished")
        else:
            raise RuntimeError(f"Copy job failed: error code {exit_code}")

        return self.destination
