import asyncio
import base64
import json
import logging
import re
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, MutableMapping, Sequence

import click
import toml
from neuromation import api as neuro_api
from neuromation.api.url_utils import normalize_storage_path_uri, uri_from_cli
from neuromation.cli.asyncio_utils import run as run_async
from yarl import URL


logger = logging.getLogger(__file__)


ASSETS_PATH = Path(__file__).resolve().parent / "assets"
SELDON_CUSTOM_PATH = ASSETS_PATH / "seldon.package"


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

    async def _create_docker_config(self) -> DockerConfig:
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

    async def _save_docker_config(self, docker_config: DockerConfig, uri: URL) -> None:
        async def _gen() -> AsyncIterator[bytes]:
            yield json.dumps(docker_config.to_primitive()).encode()

        await self._client.storage.create(uri, _gen())

    def _create_builder_container(
        self, *, docker_config_uri: URL, context_uri: URL, image_ref: str
    ) -> neuro_api.Container:
        return neuro_api.Container(
            image=neuro_api.RemoteImage(
                name="gcr.io/kaniko-project/executor", tag="latest",
            ),
            resources=neuro_api.Resources(cpu=1.0, memory_mb=4096),
            volumes=[
                neuro_api.Volume(
                    docker_config_uri, "/kaniko/.docker/config.json", read_only=True
                ),
                # TODO: try read only
                neuro_api.Volume(context_uri, "/workspace"),
            ],
            command=f"--destination={image_ref}",
        )

    def _parse_image_ref(self, image_uri_str: str) -> str:
        image = self._client.parse.remote_image(image_uri_str)
        return re.sub(r"^http[s]?://", "", image.as_docker_url())

    async def launch(
        self, context_uri: URL, image_uri_str: str
    ) -> neuro_api.JobDescription:
        # TODO: check if Dockerfile exists

        logging.info(f"Using {context_uri} as the build context")

        build_uri = self._generate_build_uri()
        await self._client.storage.mkdir(build_uri, parents=True, exist_ok=True)

        if context_uri.scheme == "file":
            local_context_uri, context_uri = context_uri, build_uri / "context"
            logger.info(f"Uploading {local_context_uri} to {context_uri}")
            await self._client.storage.upload_dir(local_context_uri, context_uri)

        docker_config = await self._create_docker_config()
        docker_config_uri = build_uri / ".docker.config.json"
        logger.debug(f"Uploading {docker_config_uri}")
        await self._save_docker_config(docker_config, docker_config_uri)

        logger.info(f"Submitting a builder job")
        image_ref = self._parse_image_ref(image_uri_str)
        builder_container = self._create_builder_container(
            docker_config_uri=docker_config_uri,
            context_uri=context_uri,
            image_ref=image_ref,
        )
        # TODO: set proper tags
        job = await self._client.jobs.run(builder_container)
        logger.info(f"The builder job ID: {job.id}")
        return job


@click.group()
def main() -> None:
    logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


@main.group()
def image() -> None:
    pass


@image.command("build")
@click.argument("context")
@click.argument("image_uri")
def image_build(context: str, image_uri: str) -> None:
    run_async(_build_image(context, image_uri))


async def _build_image(context: str, image_uri: str) -> None:
    async with neuro_api.get() as client:
        context_uri = uri_from_cli(
            context,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        builder = ImageBuilder(client)
        job = await builder.launch(context_uri, image_uri)
        while job.status == neuro_api.JobStatus.PENDING:
            job = await client.jobs.status(job.id)
            await asyncio.sleep(1.0)
        async for chunk in client.jobs.monitor(job.id):
            if not chunk:
                break
            click.echo(chunk.decode(errors="ignore"), nl=False)
        job = await client.jobs.status(job.id)
        if job.status == neuro_api.JobStatus.FAILED:
            logger.error("The builder job has failed due to:")
            logger.error(f"  Reason: {job.history.reason}")
            logger.error(f"  Description: {job.history.description}")
        else:
            logger.info(f"Successfully built {image_uri}")


@main.group()
def seldon() -> None:
    pass


@seldon.command("init-package")
@click.argument("path")
def seldon_init_package(path: str) -> None:
    run_async(_init_seldon_package(path))


async def _init_seldon_package(path: str) -> None:
    async with neuro_api.get() as client:
        uri = uri_from_cli(
            path,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        click.echo(f"Copying a Seldon package scaffolding into {uri}")
        if uri.scheme == "file":
            shutil.copytree(SELDON_CUSTOM_PATH, path)
        else:
            await client.storage.mkdir(uri, parents=True)
            await client.storage.upload_dir(URL(SELDON_CUSTOM_PATH.as_uri()), uri)


@main.command("init-aliases")
def init_aliases() -> None:
    # TODO: support patching the global ~/.neuro/user.toml
    toml_path = Path.cwd() / ".neuro.toml"
    config: MutableMapping[str, Any] = {}
    if toml_path.exists():
        with toml_path.open("r") as f:
            config = toml.load(f)
    config.setdefault("alias", {})
    config["alias"]["image-build"] = {
        "exec": "neuro-extras image build {context} {image_uri}",
        "args": "CONTEXT IMAGE_URI",
    }
    config["alias"]["seldon-init-package"] = {
        "exec": "neuro-extras seldon init-package {uri_or_path}",
        "args": "URI_OR_PATH",
    }
    with toml_path.open("w") as f:
        toml.dump(config, f)
    logger.info(f"Added aliases to {toml_path}")
