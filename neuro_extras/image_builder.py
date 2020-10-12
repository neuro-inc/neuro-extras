import asyncio
import base64
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Sequence

import click
from neuromation import api as neuro_api
from neuromation.api.url_utils import normalize_storage_path_uri
from yarl import URL


logger = logging.getLogger(__name__)


@dataclass
class DockerConfigAuth:
    registry: str
    username: str
    password: str = field(repr=False)

    @property
    def credentials(self) -> str:
        return base64.b64encode(f"{self.username}:{self.password}".encode()).decode()


@dataclass
class DockerConfig:
    auths: Sequence[DockerConfigAuth] = ()

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "auths": {auth.registry: {"auth": auth.credentials} for auth in self.auths}
        }


class ImageBuilder:
    def __init__(self, client: neuro_api.Client) -> None:
        self._client = client

    def _generate_build_uri(self) -> URL:
        return normalize_storage_path_uri(
            URL(f"storage:.builds/{uuid.uuid4()}"),
            self._client.username,
            self._client.cluster_name,
        )

    def _get_registry(self) -> str:
        url = self._client.config.registry_url
        if url.explicit_port:  # type: ignore
            return f"{url.host}:{url.explicit_port}"  # type: ignore
        return url.host  # type: ignore

    async def create_docker_config(self) -> DockerConfig:
        config = self._client.config
        token = await config.token()
        return DockerConfig(
            auths=[
                DockerConfigAuth(
                    registry=self._get_registry(),
                    username=config.username,
                    password=token,
                )
            ]
        )

    async def save_docker_config(self, docker_config: DockerConfig, uri: URL) -> None:
        async def _gen() -> AsyncIterator[bytes]:
            yield json.dumps(docker_config.to_primitive()).encode()

        await self._client.storage.create(uri, _gen())

    async def _create_builder_container(
        self,
        *,
        docker_config_uri: URL,
        context_uri: URL,
        dockerfile_path: str,
        image_ref: str,
        build_args: Sequence[str] = (),
        volume: Sequence[str],
        env: Sequence[str],
    ) -> neuro_api.Container:

        cache_image = neuro_api.RemoteImage(
            name="layer-cache/cache",
            owner=self._client.config.username,
            registry=str(self._client.config.registry_url),
            cluster_name=self._client.cluster_name,
        )
        cache_repo = self.parse_image_ref(str(cache_image))
        cache_repo = re.sub(r":.*$", "", cache_repo)
        command = (
            f"--dockerfile={dockerfile_path} --destination={image_ref} "
            f"--cache=true --cache-repo={cache_repo}"
            " --snapshotMode=redo --verbosity=debug"
        )

        if build_args:
            command += "".join([f" --build-arg {arg}" for arg in build_args])

        env_parse_result = self._client.parse.envs(env)
        vol = self._client.parse.volumes(volume)
        volumes, secret_files, disk_volumes = (
            list(vol.volumes),
            list(vol.secret_files),
            list(vol.disk_volumes),
        )

        command += "".join(
            [f" --build-arg {arg}" for arg in env_parse_result.env.keys()]
        )
        command += "".join(
            [f" --build-arg {arg}" for arg in env_parse_result.secret_env.keys()]
        )

        default_volumes = [
            neuro_api.Volume(
                docker_config_uri, "/kaniko/.docker/config.json", read_only=True
            ),
            # TODO: try read only
            neuro_api.Volume(context_uri, "/workspace"),
        ]

        volumes.extend(default_volumes)

        return neuro_api.Container(
            image=neuro_api.RemoteImage(
                name="gcr.io/kaniko-project/executor",
                tag="latest",
            ),
            resources=neuro_api.Resources(cpu=1.0, memory_mb=4096),
            command=command,
            volumes=volumes,
            disk_volumes=disk_volumes,
            secret_files=secret_files,
            env=env_parse_result.env,
            secret_env=env_parse_result.secret_env,
        )

    def parse_image_ref(self, image_uri_str: str) -> str:
        image = self._client.parse.remote_image(image_uri_str)
        return re.sub(r"^http[s]?://", "", image.as_docker_url())

    async def launch(
        self,
        dockerfile_path: str,
        context_uri: URL,
        image_uri_str: str,
        build_args: Sequence[str],
        volume: Sequence[str],
        env: Sequence[str],
    ) -> neuro_api.JobDescription:
        # TODO: check if Dockerfile exists

        logging.info(f"Using {context_uri} as the build context")

        build_uri = self._generate_build_uri()
        await self._client.storage.mkdir(build_uri, parents=True, exist_ok=True)

        if context_uri.scheme == "file":
            local_context_uri, context_uri = context_uri, build_uri / "context"
            logger.info(f"Uploading {local_context_uri} to {context_uri}")
            subprocess = await asyncio.create_subprocess_exec(
                "neuro", "cp", "--recursive", str(local_context_uri), str(context_uri)
            )
            return_code = await subprocess.wait()
            if return_code != 0:
                raise click.ClickException("Uploading build context failed!")

        docker_config = await self.create_docker_config()
        docker_config_uri = build_uri / ".docker.config.json"
        logger.debug(f"Uploading {docker_config_uri}")
        await self.save_docker_config(docker_config, docker_config_uri)

        logger.info("Submitting a builder job")
        image_ref = self.parse_image_ref(image_uri_str)
        builder_container = await self._create_builder_container(
            docker_config_uri=docker_config_uri,
            context_uri=context_uri,
            dockerfile_path=dockerfile_path,
            image_ref=image_ref,
            build_args=build_args,
            volume=volume,
            env=env,
        )
        # TODO: set proper tags
        job = await self._client.jobs.run(builder_container, life_span=4 * 60 * 60)
        logger.info(f"The builder job ID: {job.id}")
        return job
