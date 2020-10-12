import logging
from typing import Sequence

from neuromation import api as neuro_api
from yarl import URL


logger = logging.getLogger(__name__)

NEURO_EXTRAS_IMAGE_TAG = "v20.10.16a1"
NEURO_EXTRAS_IMAGE = f"neuromation/neuro-extras:{NEURO_EXTRAS_IMAGE_TAG}"


class DataCopier:
    def __init__(self, client: neuro_api.Client):
        self._client = client

    async def launch(
        self,
        extract: bool,
        src_uri: URL,
        dst_uri: URL,
        volume: Sequence[str],
        env: Sequence[str],
    ) -> neuro_api.JobDescription:
        logger.info("Submitting a copy job")
        copier_container = await self._create_copier_container(
            extract, src_uri, dst_uri, volume, env
        )
        job = await self._client.jobs.run(copier_container, life_span=60 * 60)
        logger.info(f"The copy job ID: {job.id}")
        return job

    async def _create_copier_container(
        self,
        extract: bool,
        src_uri: URL,
        dst_uri: URL,
        volume: Sequence[str],
        env: Sequence[str],
    ) -> neuro_api.Container:
        args = f"{str(src_uri)} {str(dst_uri)}"
        if extract:
            args = f"-x {args}"

        env_parse_result = self._client.parse.envs(env)
        vol = self._client.parse.volumes(volume)
        volumes, secret_files, disk_volumes = (
            list(vol.volumes),
            list(vol.secret_files),
            list(vol.disk_volumes),
        )

        gcp_env = "GOOGLE_APPLICATION_CREDENTIALS"
        cmd = (
            f'( [ "${gcp_env}" ] && '
            f"gcloud auth activate-service-account --key-file ${gcp_env} ) ; "
            f"neuro-extras data cp {args}"
        )
        return neuro_api.Container(
            image=neuro_api.RemoteImage.new_external_image(NEURO_EXTRAS_IMAGE),
            resources=neuro_api.Resources(cpu=2.0, memory_mb=4096),
            volumes=volumes,
            disk_volumes=disk_volumes,
            command=f"bash -c '{cmd} '",
            env=env_parse_result.env,
            secret_env=env_parse_result.secret_env,
            secret_files=secret_files,
        )
