import asyncio
import base64
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Sequence

import click
import neuro_sdk as neuro_api
from neuro_sdk import Preset, Resources
from neuro_sdk.url_utils import normalize_storage_path_uri
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
    def __init__(
        self,
        client: neuro_api.Client,
        other_clients_configs: Sequence[neuro_api.Config] = (),
        verbose: bool = False,
    ) -> None:
        self._client = client
        self._other_clients_configs = list(other_clients_configs)
        self._verbose = verbose

    @property
    def _all_configs(self) -> Sequence[neuro_api.Config]:
        return [self._client.config] + self._other_clients_configs

    def _generate_build_uri(self) -> URL:
        return normalize_storage_path_uri(
            URL(f"storage:.builds/{uuid.uuid4()}"),
            self._client.username,
            self._client.cluster_name,
        )

    def _get_registry(self, config: neuro_api.Config) -> str:
        url = config.registry_url
        if url.explicit_port:  # type: ignore
            return f"{url.host}:{url.explicit_port}"  # type: ignore
        return url.host  # type: ignore

    async def create_docker_config(self) -> DockerConfig:
        return DockerConfig(
            auths=[
                DockerConfigAuth(
                    registry=self._get_registry(config),
                    username=config.username,
                    password=await config.token(),
                )
                for config in self._all_configs
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
        use_cache: bool = True,
        build_args: Sequence[str] = (),
        volume: Sequence[str],
        env: Sequence[str],
        job_preset: Preset,
    ) -> neuro_api.Container:

        cache_image = neuro_api.RemoteImage(
            name="layer-cache/cache",
            owner=self._client.config.username,
            registry=str(self._client.config.registry_url),
            cluster_name=self._client.cluster_name,
        )
        cache_repo = self.parse_image_ref(str(cache_image))
        cache_repo = re.sub(r":.*$", "", cache_repo)
        container_context_path = "/kaniko_context"
        verbosity = "debug" if self._verbose else "info"
        cache = "true" if use_cache else "false"
        args = [
            f"--dockerfile={container_context_path}/{dockerfile_path}",
            f"--destination={image_ref}",
            f"--cache={cache}",
            f"--cache-repo={cache_repo}",
            f"--snapshotMode=redo",
            f" --verbosity={verbosity}",
            f" --context={container_context_path}",
        ]

        for arg in build_args:
            args.append(f" --build-arg {arg}")

        env_parsed = self._client.parse.envs(env)
        for arg in list(env_parsed.env) + list(env_parsed.secret_env):
            args.append(f"--build-arg {arg}")

        vol = self._client.parse.volumes(volume)
        volumes, secret_files, disk_volumes = (
            list(vol.volumes),
            list(vol.secret_files),
            list(vol.disk_volumes),
        )

        default_volumes = [
            neuro_api.Volume(
                docker_config_uri, "/kaniko/.docker/config.json", read_only=True
            ),
            # context dir cannot be R/O if we want to mount secrets there
            neuro_api.Volume(context_uri, container_context_path, read_only=False),
        ]

        volumes.extend(default_volumes)

        resources = Resources(
            memory_mb=job_preset.memory_mb,
            cpu=job_preset.cpu,
            gpu=job_preset.gpu,
            gpu_model=job_preset.gpu_model,
            tpu_type=job_preset.tpu_type,
            tpu_software_version=job_preset.tpu_software_version,
        )
        return neuro_api.Container(
            image=neuro_api.RemoteImage(
                name="gcr.io/kaniko-project/executor",
                tag="v1.1.0",
            ),
            resources=resources,
            command=" ".join(args),
            volumes=volumes,
            disk_volumes=disk_volumes,
            secret_files=secret_files,
            env=env_parsed.env,
            secret_env=env_parsed.secret_env,
        )

    def parse_image_ref(self, image_uri_str: str) -> str:
        image = self._client.parse.remote_image(image_uri_str)
        return re.sub(r"^http[s]?://", "", image.as_docker_url())

    async def launch(
        self,
        dockerfile_path: str,
        context_uri: URL,
        image_uri_str: str,
        use_cache: bool,
        build_args: Sequence[str],
        volume: Sequence[str],
        env: Sequence[str],
        job_preset: Preset,
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
            use_cache=use_cache,
            build_args=build_args,
            volume=volume,
            env=env,
            job_preset=job_preset,
        )
        # TODO: set proper tags
        job = await self._client.jobs.run(builder_container, life_span=4 * 60 * 60)
        logger.info(f"The builder job ID: {job.id}")
        return job
