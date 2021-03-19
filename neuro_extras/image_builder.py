import asyncio
import base64
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional, Sequence, Tuple

import click
import neuro_sdk as neuro_api
from neuro_sdk.url_utils import normalize_storage_path_uri
from yarl import URL


KANIKO_IMAGE_REF = "gcr.io/kaniko-project/executor"
KANIKO_IMAGE_TAG = "v1.5.1"
KANIKO_CONTEXT_PATH = "/kaniko_context"
BUILDER_JOB_LIFESPAN = "4h"
BUILDER_JOB_SHEDULE_TIMEOUT = "20m"

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

    def parse_image_ref(self, image_uri_str: str) -> str:
        image = self._client.parse.remote_image(image_uri_str)
        return re.sub(r"^http[s]?://", "", image.as_docker_url())

    async def build(
        self,
        dockerfile_path: Path,
        context_uri: URL,
        image_uri_str: str,
        use_cache: bool,
        build_args: Tuple[str, ...],
        volumes: Tuple[str, ...],
        envs: Tuple[str, ...],
        job_preset: Optional[str],
        build_tags: Tuple[str, ...],
    ) -> int:
        # TODO: check if Dockerfile exists
        logger.info(f"Building the image {image_uri_str}")
        logger.info(f"Using {context_uri} as the build context")

        # upload (if needed) build context and platform registry auth info
        build_uri = self._generate_build_uri()
        await self._client.storage.mkdir(build_uri, parents=True)
        if context_uri.scheme == "file":
            local_context_uri, context_uri = context_uri, build_uri / "context"
            logger.info(f"Uploading {local_context_uri} to {context_uri}")
            subprocess = await asyncio.create_subprocess_exec(
                "neuro",
                "--disable-pypi-version-check",
                "cp",
                "--recursive",
                str(local_context_uri),
                str(context_uri),
            )
            return_code = await subprocess.wait()
            if return_code != 0:
                raise click.ClickException("Uploading build context failed!")

        docker_config = await self.create_docker_config()
        docker_config_uri = build_uri / ".docker.config.json"
        logger.debug(f"Uploading {docker_config_uri}")
        await self.save_docker_config(docker_config, docker_config_uri)

        cache_image = neuro_api.RemoteImage(
            name="layer-cache/cache",
            owner=self._client.config.username,
            registry=str(self._client.config.registry_url),
            cluster_name=self._client.cluster_name,
        )
        cache_repo = self.parse_image_ref(str(cache_image))
        cache_repo = re.sub(r":.*$", "", cache_repo)  # drop tag

        # mount build context and Kaniko auth info
        volumes += (
            f"{docker_config_uri}:/kaniko/.docker/config.json:ro",
            # context dir cannot be R/O if we want to mount secrets there
            f"{context_uri}:{KANIKO_CONTEXT_PATH}:rw",
        )
        dst_image = self._client.parse.remote_image(image_uri_str)
        build_tags += (f"kaniko-builds-image:{dst_image}",)
        kaniko_args = [
            f"--dockerfile={KANIKO_CONTEXT_PATH}/{dockerfile_path}",
            f"--destination={self.parse_image_ref(image_uri_str)}",
            f"--cache={'true' if use_cache else 'false'}",
            f"--cache-copy-layers",
            f"--cache-repo={cache_repo}",
            f"--snapshotMode=redo",
            f"--verbosity={'debug' if self._verbose else 'info'}",
            f"--context={KANIKO_CONTEXT_PATH}",
        ]

        for arg in build_args:
            kaniko_args.append(f"--build-arg {arg}")
        # env vars (which might be platform secrets too) are passed as build args
        env_parsed = self._client.parse.envs(envs)
        for arg in list(env_parsed.env) + list(env_parsed.secret_env):
            kaniko_args.append(f"--build-arg {arg}")

        build_command = [
            "neuro",
            "--disable-pypi-version-check",
            "job",
            "run",
            f"--life-span={BUILDER_JOB_LIFESPAN}",
            f"--schedule-timeout={BUILDER_JOB_SHEDULE_TIMEOUT}",
        ]
        if job_preset:
            build_command.append(f"--preset={job_preset}")
        for volume in volumes:
            build_command.append(f"--volume={volume}")
        for env in envs:
            build_command.append(f"--env={env}")
        for build_tag in build_tags:
            build_command.append(f"--tag={build_tag}")
        build_command.append(f"{KANIKO_IMAGE_REF}:{KANIKO_IMAGE_TAG}")
        build_command.append(" ".join(kaniko_args))

        logger.info("Submitting a builder job")
        logger.debug(build_command)
        subprocess = await asyncio.create_subprocess_exec(*build_command)
        # TODO: remove context after the build is finished?
        return await subprocess.wait()
