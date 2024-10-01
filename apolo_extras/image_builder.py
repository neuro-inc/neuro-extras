import asyncio
import base64
import json
import logging
import re
import shlex
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, Tuple, Type

import apolo_sdk
import click
from apolo_cli.formatters.images import DockerImageProgress
from apolo_sdk._url_utils import _extract_path
from rich.console import Console
from yarl import URL


KANIKO_IMAGE_REF = "gcr.io/kaniko-project/executor"
KANIKO_IMAGE_TAG = "v1.20.0-debug"  # debug has busybox, which is needed for auth
KANIKO_AUTH_PREFIX = "NE_REGISTRY_AUTH"
KANIKO_DOCKER_CONFIG_PATH = "/kaniko/.docker/config.json"
KANIKO_AUTH_SCRIPT_PATH = "/kaniko/.docker/merge_docker_auths.sh"
KANIKO_CONTEXT_PATH = "/kaniko_context"
KANIKO_EXTRA_ENVS = ("container=docker",)
BUILDER_JOB_LIFESPAN = "4h"
BUILDER_JOB_SHEDULE_TIMEOUT = "20m"

MIN_BUILD_PRESET_CPU: float = 2
MIN_BUILD_PRESET_MEM: int = 4096

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


async def create_docker_config_auth(
    client_config: apolo_sdk.Config,
) -> DockerConfigAuth:
    # retrieve registry hostname with optional port
    url = client_config.registry_url
    assert url.host
    port = f":{url.explicit_port}" if url.explicit_port else ""
    registry_host = url.host + port
    auth = DockerConfigAuth(
        registry=registry_host,
        username=client_config.username,
        password=await client_config.token(),
    )
    return auth


class ImageBuilder(ABC):
    def __init__(
        self,
        client: apolo_sdk.Client,
        extra_registry_auths: Sequence[DockerConfigAuth] = (),
        verbose: bool = False,
    ) -> None:
        """
            Builds and pushes docker image to the platform.
            By default, build  happens on the platform, using Kaniko tool,
            unless --local is specified.

        Args:
            client (apolo_sdk.Client): platform client instance of apolo-sdk,
                authenticated to the destination cluster
            extra_registry_auths (Sequence[DockerConfigAuth], optional):
                Sequence of extra docker container registry auth credits,
                useful if base image(s) hidden under private registry(es).
                Defaults to ().
            verbose (bool, optional): Whether to set Kaniko's verbosity to DEBUG.
                Defaults to False.
        """
        self._client = client
        self._extra_registry_auths = list(extra_registry_auths)
        self._verbose = verbose

    def _generate_build_uri(self, project_name: str) -> URL:
        return self._client.parse.normalize_uri(
            URL(f"storage:/{project_name}/.builds/{uuid.uuid4()}"),
        )

    async def create_docker_config(self) -> DockerConfig:
        dst_reg_auth = await create_docker_config_auth(self._client.config)
        return DockerConfig(auths=[dst_reg_auth] + self._extra_registry_auths)

    async def save_docker_config(self, docker_config: DockerConfig, uri: URL) -> None:
        async def _gen() -> AsyncIterator[bytes]:
            yield json.dumps(docker_config.to_primitive()).encode()

        await self._client.storage.create(uri, _gen())

    def parse_image_ref(self, image_uri_str: str) -> str:
        image = self._client.parse.remote_image(image_uri_str)
        return re.sub(r"^http[s]?://", "", image.as_docker_url())

    @abstractmethod
    async def build(
        self,
        dockerfile_path: Path,
        context_uri: URL,
        image: apolo_sdk.RemoteImage,
        use_cache: bool,
        build_args: Tuple[str, ...],
        volumes: Tuple[str, ...],
        envs: Tuple[str, ...],
        job_preset: Optional[str],
        build_tags: Tuple[str, ...],
        project_name: str,
        extra_kaniko_args: Optional[str],
    ) -> int:
        pass

    @staticmethod
    def get(local: bool) -> Type["ImageBuilder"]:
        if local:
            return LocalImageBuilder
        else:
            return RemoteImageBuilder

    async def _execute_subprocess(self, command: Sequence[str]) -> int:
        logger.debug("Executing subprocess: %s", " ".join(command))
        subprocess = await asyncio.create_subprocess_exec(*command)
        return await subprocess.wait()


class LocalImageBuilder(ImageBuilder):
    async def build(
        self,
        dockerfile_path: Path,
        context_uri: URL,
        image: apolo_sdk.RemoteImage,
        use_cache: bool,
        build_args: Tuple[str, ...],
        volumes: Tuple[str, ...],
        envs: Tuple[str, ...],
        job_preset: Optional[str],
        build_tags: Tuple[str, ...],
        project_name: str,
        extra_kaniko_args: Optional[str],
    ) -> int:
        logger.info(f"Building the image {image}")
        logger.info(f"Using {context_uri} as the build context")
        if extra_kaniko_args:
            logger.warning(
                "Extra kaniko args are not supported for local builds. "
                "They will be ignored."
            )

        docker_build_args = []

        for arg in build_args:
            docker_build_args.append(f"--build-arg {arg}")

        build_command = [
            "docker",
            "build",
            f"--tag={image.as_docker_url()}",
            f"--file={dockerfile_path}",
        ]
        if not self._verbose:
            build_command.append("--quiet")
        if len(docker_build_args) > 0:
            build_command.append(" ".join(docker_build_args))
        build_command.append(str(_extract_path(context_uri)))

        ex_code = await self._execute_subprocess(build_command)
        if ex_code != 0:
            return ex_code
        return await self._push_image(image)

    async def _push_image(self, image: apolo_sdk.RemoteImage) -> int:
        logger.info(f"Pushing image to registry")
        console = Console()
        progress = DockerImageProgress.create(console=console, quiet=not self._verbose)
        local_image = self._client.parse.local_image(image.as_docker_url())
        try:
            await self._client.images.push(local_image, image, progress=progress)
            logger.info(
                f"Pushed {image.as_docker_url()} to the platform registry as {image}"
            )
        except Exception as e:
            logger.exception("Image push failed.")
            logger.info(
                f"You may try to repeat the push process by running "
                f"'apolo image push {local_image} {image}'"
            )
            raise e
        return 0


class RemoteImageBuilder(ImageBuilder):
    async def build(
        self,
        dockerfile_path: Path,
        context_uri: URL,
        image: apolo_sdk.RemoteImage,
        use_cache: bool,
        build_args: Tuple[str, ...],
        volumes: Tuple[str, ...],
        envs: Tuple[str, ...],
        job_preset: Optional[str],
        build_tags: Tuple[str, ...],
        project_name: str,
        extra_kaniko_args: Optional[str],
    ) -> int:
        # TODO: check if Dockerfile exists
        logger.info(f"Building the image {image}")
        logger.info(f"Using {context_uri} as the build context")

        # upload (if needed) build context and platform registry auth info
        build_uri = self._generate_build_uri(project_name)
        await self._client.storage.mkdir(build_uri, parents=True)
        if context_uri.scheme == "file":
            storage_context_uri = build_uri / "context"
            await self._upload_to_storage(context_uri, storage_context_uri)
            context_uri = storage_context_uri

        docker_config = await self.create_docker_config()
        docker_config_uri = build_uri / ".docker.config.json"
        logger.debug(f"Uploading {docker_config_uri}")
        await self.save_docker_config(docker_config, docker_config_uri)

        cache_image = apolo_sdk.RemoteImage(
            name="layer-cache/cache",
            project_name=project_name,
            registry=str(self._client.config.registry_url),
            cluster_name=self._client.cluster_name,
            org_name=self._client.config.org_name,
        )
        cache_repo = self.parse_image_ref(str(cache_image))
        cache_repo = re.sub(r":.*$", "", cache_repo)  # drop tag

        if any(KANIKO_AUTH_PREFIX in env for env in envs):
            # we have extra auth info.
            # in this case we cannot mount registry auth info at the default path
            # and should upload and configure 'merge_docker_auths' script to merge auths
            mnt_path = Path(KANIKO_DOCKER_CONFIG_PATH)
            mnt_path = mnt_path.with_name(f"{mnt_path.stem}_base{mnt_path.suffix}")
            docker_config_mnt = str(mnt_path)
            envs += (
                f"{KANIKO_AUTH_PREFIX}_BASE_{uuid.uuid4().hex[:8]}={docker_config_mnt}",
            )
            local_script = URL(
                (Path(__file__).parent / "assets" / "merge_docker_auths.sh").as_uri()
            )
            remote_script = build_uri / "merge_docker_auths.sh"
            await self._client.storage.upload_file(local_script, remote_script)
            volumes += (f"{remote_script}:{KANIKO_AUTH_SCRIPT_PATH}:ro",)
            job_entrypoint_overwrite = [
                f"sh {KANIKO_AUTH_SCRIPT_PATH}",
                "&&",
                "executor",
                # Kaniko args will be added below
            ]
        else:
            docker_config_mnt = str(KANIKO_DOCKER_CONFIG_PATH)
            job_entrypoint_overwrite = []

        # mount build context and platform registry auth info
        volumes += (
            f"{docker_config_uri}:{docker_config_mnt}:ro",
            # context dir cannot be R/O if we want to mount secrets there
            f"{context_uri}:{KANIKO_CONTEXT_PATH}:rw",
        )
        build_tags += (f"kaniko-builds-image:{image}",)
        kaniko_args = [
            f"--context={KANIKO_CONTEXT_PATH}",
            f"--dockerfile={KANIKO_CONTEXT_PATH}/{dockerfile_path.as_posix()}",
            f"--destination={image.as_docker_url(with_scheme=False)}",
            f"--cache={'true' if use_cache else 'false'}",
            f"--cache-repo={cache_repo}",
            f"--verbosity={'debug' if self._verbose else 'info'}",
            "--image-fs-extract-retry=1",
            "--push-retry=3",
            "--use-new-run=true",
            "--snapshot-mode=redo",
        ]

        for arg in build_args:
            kaniko_args.append(f"--build-arg {arg}")
        # env vars (which might be platform secrets too) are passed as build args
        env_parsed = self._client.parse.envs(envs)
        for arg in list(env_parsed.env) + list(env_parsed.secret_env):
            if KANIKO_AUTH_PREFIX not in arg:
                kaniko_args.append(f"--build-arg {arg}")

        kaniko_args = self._add_extra_kaniko_args(kaniko_args, extra_kaniko_args)

        build_command = [
            "apolo",
            "--disable-pypi-version-check",
            "job",
            "run",
            f"--life-span={BUILDER_JOB_LIFESPAN}",
            f"--schedule-timeout={BUILDER_JOB_SHEDULE_TIMEOUT}",
            f"--project={project_name}",
        ]
        if job_preset:
            build_command.append(f"--preset={job_preset}")
        for build_tag in build_tags:
            build_command.append(f"--tag={build_tag}")
        for volume in volumes:
            build_command.append(f"--volume={volume}")
        for env in envs:
            build_command.append(f"--env={env}")
        envs_keys = [e.split("=")[0] for e in envs]
        for extra_env in KANIKO_EXTRA_ENVS:
            if extra_env.split("=")[0] in envs_keys:
                logger.warning(
                    f"Cannot overwite env {extra_env}: already present. "
                    "Consider removing this environment variable from your config, "
                    "otherwise, the build might fail."
                )
            else:
                build_command.append(f"--env={extra_env}")

        kaniko_args_str = " ".join(kaniko_args)
        if job_entrypoint_overwrite:
            job_entrypoint_overwrite.append(kaniko_args_str)
            build_command.append("--entrypoint")
            build_command.append(
                f"sh -c {shlex.quote(' '.join(job_entrypoint_overwrite))}"
            )
            build_command.append(f"{KANIKO_IMAGE_REF}:{KANIKO_IMAGE_TAG}")
        else:
            build_command.append(f"{KANIKO_IMAGE_REF}:{KANIKO_IMAGE_TAG}")
            build_command.append("--")
            build_command.append(kaniko_args_str)

        # TODO: remove context after the build is finished?
        return await self._execute_subprocess(build_command)

    async def _upload_to_storage(self, local_url: URL, remote_url: URL) -> None:
        logger.info(f"Uploading {local_url} to {remote_url}")
        command = [
            "apolo",
            "--disable-pypi-version-check",
            "cp",
            "--recursive",
            str(local_url),
            str(remote_url),
        ]
        return_code = await self._execute_subprocess(command)
        if return_code != 0:
            raise click.ClickException("Uploading build context failed!")

    def _add_extra_kaniko_args(
        self, kaniko_args: List[str], extra_kaniko_args: Optional[str]
    ) -> List[str]:
        if not extra_kaniko_args:
            return kaniko_args

        extra_args = shlex.split(extra_kaniko_args)
        kaniko_arg_keys = [arg.split("=")[0] for arg in kaniko_args]
        extra_args_keys = [arg.split("=")[0] for arg in extra_args]
        overlap = set(extra_args_keys) & set(kaniko_arg_keys)
        if not overlap:
            return kaniko_args + extra_args
        raise ValueError(
            f"Extra kaniko arguments {overlap} overlap with autogenerated arguments. "
            "Please remove them in order to proceed or contact the support team."
        )
