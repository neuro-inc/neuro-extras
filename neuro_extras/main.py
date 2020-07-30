import asyncio
import base64
import json
import logging
import re
import sys
import tempfile
import textwrap
import uuid
from dataclasses import dataclass, field
from distutils import dir_util
from pathlib import Path
from typing import Any, AsyncIterator, Dict, MutableMapping, Sequence

import click
import toml
import yaml
from neuromation import api as neuro_api
from neuromation.api.parsing_utils import _as_repo_str
from neuromation.api.url_utils import normalize_storage_path_uri, uri_from_cli
from neuromation.cli.asyncio_utils import run as run_async
from neuromation.cli.const import EX_PLATFORMERROR
from yarl import URL


logger = logging.getLogger(__name__)


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
        )

        if build_args:
            command += "".join([f" --build-arg {arg}" for arg in build_args])
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
            command=command,
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
    ) -> neuro_api.JobDescription:
        # TODO: check if Dockerfile exists

        logging.info(f"Using {context_uri} as the build context")

        build_uri = self._generate_build_uri()
        await self._client.storage.mkdir(build_uri, parents=True, exist_ok=True)

        if context_uri.scheme == "file":
            local_context_uri, context_uri = context_uri, build_uri / "context"
            logger.info(f"Uploading {local_context_uri} to {context_uri}")
            await self._client.storage.upload_dir(local_context_uri, context_uri)

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
        )
        # TODO: set proper tags
        job = await self._client.jobs.run(builder_container, life_span=60 * 60)
        logger.info(f"The builder job ID: {job.id}")
        return job


class ClickLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            click.echo(msg)
        except Exception:
            self.handleError(record)


@click.group()
def main() -> None:
    handler = ClickLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


@main.group()
def image() -> None:
    pass


@image.command("build")
@click.option("-f", "--file", default="Dockerfile")
@click.option("--build-arg", multiple=True)
@click.argument("path")
@click.argument("image_uri")
def image_build(file: str, build_arg: Sequence[str], path: str, image_uri: str) -> None:
    run_async(_build_image(file, path, image_uri, build_arg))


@image.command("copy")
@click.argument("source")
@click.argument("destination")
def image_copy(source: str, destination: str) -> None:
    run_async(_copy_image(source, destination))


async def _copy_image(source: str, destination: str) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        async with neuro_api.get() as client:
            remote_image = client.parse.remote_image(image=source)
        dockerfile_path = Path(f"{tmpdir}/Dockerfile")
        with open(str(dockerfile_path), "w") as f:
            f.write(
                textwrap.dedent(
                    f"""\
                    FROM {_as_repo_str(remote_image)}
                    LABEL neu.ro/source-image-uri={source}
                    """
                )
            )
        await _build_image("Dockerfile", tmpdir, destination, [])


async def _build_image(
    dockerfile_path: str, context: str, image_uri: str, build_args: Sequence[str]
) -> None:
    async with neuro_api.get() as client:
        context_uri = uri_from_cli(
            context,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        builder = ImageBuilder(client)
        job = await builder.launch(dockerfile_path, context_uri, image_uri, build_args)
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
            exit_code = job.history.exit_code
            if exit_code is None:
                exit_code = EX_PLATFORMERROR
            sys.exit(exit_code)
        else:
            logger.info(f"Successfully built {image_uri}")


@main.group()
def seldon() -> None:
    pass


@seldon.command("init-package")
@click.argument("path", default=".")
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
            dir_util.copy_tree(str(SELDON_CUSTOM_PATH), path)
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
        "exec": "neuro-extras image build",
        "options": [
            "-f, --file path to the Dockerfile within CONTEXT",
            "--build-arg build arguments for Docker",
        ],
        "args": "CONTEXT IMAGE_URI",
    }
    config["alias"]["seldon-init-package"] = {
        "exec": "neuro-extras seldon init-package",
        "args": "URI_OR_PATH",
    }
    config["alias"]["image-copy"] = {
        "exec": "neuro-extras image copy",
        "args": "SOURCE DESTINATION",
    }
    with toml_path.open("w") as f:
        toml.dump(config, f)
    logger.info(f"Added aliases to {toml_path}")


@main.group()
def config() -> None:
    pass


@config.command("save-docker-json")
@click.argument("path")
def config_save_docker_json(path: str) -> None:
    run_async(_save_docker_json(path))


async def _save_docker_json(path: str) -> None:
    async with neuro_api.get() as client:
        uri = uri_from_cli(
            path,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        builder = ImageBuilder(client)
        docker_config = await builder.create_docker_config()
        click.echo(f"Saving Docker config.json as {uri}")
        if uri.scheme == "file":
            with open(path, "w") as f:
                json.dump(docker_config.to_primitive(), f)
        else:
            await builder.save_docker_config(docker_config, uri)


async def _create_k8s_registry_secret(name: str) -> Dict[str, Any]:
    async with neuro_api.get() as client:
        builder = ImageBuilder(client)
        docker_config = await builder.create_docker_config()
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name},
            "type": "kubernetes.io/dockerconfigjson",
            "data": {
                ".dockerconfigjson": base64.b64encode(
                    json.dumps(docker_config.to_primitive()).encode()
                ).decode(),
            },
        }


async def _create_k8s_secret(name: str) -> Dict[str, Any]:
    async with neuro_api.get() as client:
        payload: Dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name},
            "type": "Opaque",
            "data": {},
        }
        config_path = Path(client.config._path)
        for path in config_path.iterdir():
            payload["data"][path.name] = base64.b64encode(path.read_bytes()).decode()
        return payload


async def _create_seldon_deployment(
    *,
    name: str,
    neuro_secret_name: str,
    registry_secret_name: str,
    model_image_uri: str,
    model_storage_uri: str,
) -> Dict[str, Any]:
    async with neuro_api.get() as client:
        builder = ImageBuilder(client)
        model_image_ref = builder.parse_image_ref(model_image_uri)

    pod_spec = {
        "volumes": [
            {"emptyDir": {}, "name": "neuro-storage"},
            {"name": "neuro-secret", "secret": {"secretName": neuro_secret_name}},
        ],
        "imagePullSecrets": [{"name": registry_secret_name}],
        "initContainers": [
            {
                "name": "neuro-download",
                "image": "neuromation/neuro-extras:latest",
                "imagePullPolicy": "Always",
                "command": ["bash", "-c"],
                "args": [
                    "cp -L -r /var/run/neuro/config /root/.neuro;"
                    "chmod 0700 /root/.neuro;"
                    "chmod 0600 /root/.neuro/db;"
                    f"neuro cp {model_storage_uri} /storage"
                ],
                "volumeMounts": [
                    {"mountPath": "/storage", "name": "neuro-storage"},
                    {"mountPath": "/var/run/neuro/config", "name": "neuro-secret"},
                ],
            }
        ],
        "containers": [
            {
                "name": "model",
                "image": model_image_ref,
                "imagePullPolicy": "Always",
                "volumeMounts": [{"mountPath": "/storage", "name": "neuro-storage"}],
            }
        ],
    }
    return {
        "apiVersion": "machinelearning.seldon.io/v1",
        "kind": "SeldonDeployment",
        "metadata": {"name": name},
        "spec": {
            "predictors": [
                {
                    "componentSpecs": [{"spec": pod_spec}],
                    "graph": {
                        "endpoint": {"type": "REST"},
                        "name": "model",
                        "type": "MODEL",
                    },
                    "name": "predictor",
                    "replicas": 1,
                }
            ]
        },
    }


@main.group()
def k8s() -> None:
    pass


@k8s.command("generate-secret")
@click.option("--name", default="neuro")
def generate_k8s_secret(name: str) -> None:
    payload = run_async(_create_k8s_secret(name))
    click.echo(yaml.dump(payload), nl=False)


@k8s.command("generate-registry-secret")
@click.option("--name", default="neuro-registry")
def generate_k8s_registry_secret(name: str) -> None:
    payload = run_async(_create_k8s_registry_secret(name))
    click.echo(yaml.dump(payload), nl=False)


@seldon.command("generate-deployment")
@click.option("--name", default="neuro-model")
@click.option("--neuro-secret", default="neuro")
@click.option("--registry-secret", default="neuro-registry")
@click.argument("model-image-uri")
@click.argument("model-storage-uri")
def generate_seldon_deployment(
    name: str,
    neuro_secret: str,
    registry_secret: str,
    model_image_uri: str,
    model_storage_uri: str,
) -> None:
    payload = run_async(
        _create_seldon_deployment(
            name=name,
            neuro_secret_name=neuro_secret,
            registry_secret_name=registry_secret,
            model_image_uri=model_image_uri,
            model_storage_uri=model_storage_uri,
        )
    )
    click.echo(yaml.dump(payload), nl=False)
