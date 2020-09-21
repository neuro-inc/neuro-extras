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
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Dict, MutableMapping, Sequence

import click
import toml
import yaml
from neuromation import api as neuro_api
from neuromation.api import ConfigError, find_project_root
from neuromation.api.config import load_user_config
from neuromation.api.parsing_utils import _as_repo_str
from neuromation.api.url_utils import normalize_storage_path_uri, uri_from_cli
from neuromation.cli.asyncio_utils import run as run_async
from neuromation.cli.const import EX_PLATFORMERROR
from yarl import URL


logger = logging.getLogger(__name__)

ASSETS_PATH = Path(__file__).resolve().parent / "assets"
SELDON_CUSTOM_PATH = ASSETS_PATH / "seldon.package"
TEMP_UNPACK_DIR = Path.home() / ".neuro" / "tmp"


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

        env_dict, secret_env_dict = self._client.parse.env(env)
        volumes, secret_files = self._client.parse.volumes(volume)

        command += "".join([f" --build-arg {arg}" for arg in env_dict.keys()])
        command += "".join([f" --build-arg {arg}" for arg in secret_env_dict.keys()])

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
                name="gcr.io/kaniko-project/executor", tag="latest",
            ),
            resources=neuro_api.Resources(cpu=1.0, memory_mb=4096),
            command=command,
            volumes=volumes,
            secret_files=secret_files,
            env=env_dict,
            secret_env=secret_env_dict,
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


class DataCopier:
    def __init__(self, client: neuro_api.Client):
        self._client = client

    async def launch(
        self, storage_uri: URL, extract: bool, src_uri: URL, dst_uri: URL,
    ) -> neuro_api.JobDescription:
        logger.info("Submitting a copy job")
        copier_container = await self._create_copier_container(
            storage_uri, extract, src_uri, dst_uri
        )
        job = await self._client.jobs.run(copier_container, life_span=60 * 60)
        logger.info(f"The copy job ID: {job.id}")
        return job

    async def _create_copier_container(
        self, storage_uri: URL, extract: bool, src_uri: URL, dst_uri: URL,
    ) -> neuro_api.Container:
        args = f"{str(src_uri)} {str(dst_uri)}"
        if extract:
            args = f"-x {args}"
        return neuro_api.Container(
            image=neuro_api.RemoteImage.new_external_image("neuromation/neuro-extras"),
            resources=neuro_api.Resources(cpu=4.0, memory_mb=4096),
            volumes=[neuro_api.Volume(storage_uri, "/var/storage")],
            entrypoint=f"neuro-extras data cp {args}",
        )


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


@main.group()
def data() -> None:
    pass


class UrlType(Enum):
    UNSUPPORTED = 0
    LOCAL = 1
    CLOUD = 2
    STORAGE = 3

    @staticmethod
    def get_type(url: URL) -> "UrlType":
        if url.scheme == "storage":
            return UrlType.STORAGE
        if url.scheme == "":
            return UrlType.LOCAL
        if url.scheme in ("s3", "gs"):
            return UrlType.CLOUD
        return UrlType.UNSUPPORTED


async def _data_cp(source: str, destination: str, extract: bool) -> None:
    source_url = URL(source)
    destination_url = URL(destination)
    source_url_type = UrlType.get_type(source_url)
    if source_url_type == UrlType.UNSUPPORTED:
        raise ValueError(f"Unsupported source URL scheme: {source_url.scheme}")
    destination_url_type = UrlType.get_type(destination_url)
    if destination_url_type == UrlType.UNSUPPORTED:
        raise ValueError(
            f"Unsupported destination URL scheme: {destination_url.scheme}"
        )

    if source_url_type == UrlType.CLOUD and destination_url_type == UrlType.CLOUD:
        raise ValueError(
            "This command can't be used to copy data between cloud providers"
        )
    if source_url_type == UrlType.STORAGE and destination_url_type == UrlType.STORAGE:
        raise ValueError(
            "This command can't be used to copy data between two storage locations"
        )

    if UrlType.STORAGE in (source_url_type, destination_url_type):
        async with neuro_api.get() as client:
            data_copier = DataCopier(client)
            if source_url_type == UrlType.STORAGE:
                job = await data_copier.launch(
                    storage_uri=source_url,
                    src_uri=URL("/var/storage"),
                    dst_uri=destination_url,
                    extract=extract,
                )
            else:
                job = await data_copier.launch(
                    storage_uri=destination_url,
                    src_uri=source_url,
                    dst_uri=URL("/var/storage"),
                    extract=extract,
                )
            while job.status == neuro_api.JobStatus.PENDING:
                job = await client.jobs.status(job.id)
                await asyncio.sleep(1.0)
            async for chunk in client.jobs.monitor(job.id):
                if not chunk:
                    break
                click.echo(chunk.decode(errors="ignore"), nl=False)
            job = await client.jobs.status(job.id)
            if job.status == neuro_api.JobStatus.FAILED:
                logger.error("The copy job has failed due to:")
                logger.error(f"  Reason: {job.history.reason}")
                logger.error(f"  Description: {job.history.description}")
                exit_code = job.history.exit_code
                if exit_code is None:
                    exit_code = EX_PLATFORMERROR
                sys.exit(exit_code)
            else:
                logger.info("Successfully copied data")
    else:
        TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
        tmp_dir_name = tempfile.mkdtemp(dir=str(TEMP_UNPACK_DIR))
        tmp_dst_url = URL.build(path=(tmp_dir_name + "/"))

        await _nonstorage_cp(source_url, tmp_dst_url)
        source_url = tmp_dst_url
        # for clarity
        source_url_type = UrlType.LOCAL

        # at this point source is always local
        if extract:
            # extract to tmp dir
            file = Path(source_url.path)
            if file.is_dir():
                file = list(file.glob("*"))[0]
            suffixes = file.suffixes
            if suffixes[-2:] == [".tar", ".gz"] or suffixes[-1] == ".tgz":
                command = "tar"
                args = ["zxvf", str(file), "-C", str(tmp_dst_url)]
            elif suffixes[-2:] == [".tar", ".bz2"] or suffixes[-1] in (".tbz2", ".tbz"):
                command = "tar"
                args = ["jxvf", str(file), "-C", str(tmp_dst_url)]
            elif suffixes[-1] == ".tar":
                command = "tar"
                args = ["xvf", str(file), "-C", str(tmp_dst_url)]
            elif suffixes[-1] == ".gz":
                command = "gunzip"
                args = [str(file), str(tmp_dst_url) + file.name[:-3]]
            elif suffixes[-1] == ".zip":
                command = "unzip"
                args = [str(file), "-d", str(tmp_dst_url)]
            else:
                raise ValueError(f"Don't know how to extract file {file.name}")

            click.echo(f"Running {command} {' '.join(args)}")
            subprocess = await asyncio.create_subprocess_exec(command, *args)
            returncode = await subprocess.wait()
            if returncode != 0:
                raise click.ClickException(f"Extraction failed: {subprocess.stderr}")
            else:
                if file.exists():
                    # gunzip removes src after extraction, while tar - not
                    file.unlink()

        # handle upload/rsync
        await _nonstorage_cp(source_url, destination_url)


async def _nonstorage_cp(source_url: URL, destination_url: URL) -> None:
    if "s3" in (source_url.scheme, destination_url.scheme):
        command = "aws"
        args = ["s3", "cp", str(source_url), str(destination_url)]
        if source_url.path.endswith("/"):
            args.insert(2, "--recursive")
    elif "gs" in (source_url.scheme, destination_url.scheme):
        command = "gsutil"
        args = ["-m", "cp", "-r", str(source_url), str(destination_url)]
    elif source_url.scheme == "" and destination_url.scheme == "":
        command = "rsync"
        args = [
            "-avzh",
            "--progress",
            "--remove-source-files",
            str(source_url),
            str(destination_url),
        ]
    else:
        raise ValueError("Unknown cloud provider")
    click.echo(f"Running {command} {' '.join(args)}")
    subprocess = await asyncio.create_subprocess_exec(command, *args)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise click.ClickException("Cloud copy failed")
    elif UrlType.get_type(source_url) == UrlType.LOCAL:
        source_path = Path(source_url.path)
        if source_path.is_dir():
            dir_util.remove_tree(str(source_path))
        else:
            source_path.unlink()


@data.command("cp")
@click.argument("source")
@click.argument("destination")
@click.option("-x", "--extract", default=False, is_flag=True)
def data_cp(source: str, destination: str, extract: bool) -> None:
    """
    Sample test commands:
    neuro-extras data cp -x s3://my-bucket/data.zip /tmp/
    neuro-extras data cp s3://sra-pub-sars-cov2/sra-src/SRR9967744/ /tmp/
    neuro-extras data cp gs://gcp-public-data--broad-references/refdisk_manifest.json \
            /tmp/refdisk_manifest.json
    neuro-extras data cp s3://sra-pub-sars-cov2/sra-src/SRR9967744/ storage:
    """
    run_async(_data_cp(source, destination, extract))


@image.command("build")
@click.option("-f", "--file", default="Dockerfile")
@click.option("--build-arg", multiple=True)
@click.option(
    "-v",
    "--volume",
    metavar="MOUNT",
    multiple=True,
    help=(
        "Mounts directory from vault into container. "
        "Use multiple options to mount more than one volume. "
        "Use --volume=ALL to mount all accessible storage directories."
    ),
)
@click.option(
    "-e",
    "--env",
    metavar="VAR=VAL",
    multiple=True,
    help=(
        "Set environment variable in container "
        "Use multiple options to define more than one variable"
    ),
)
@click.argument("path")
@click.argument("image_uri")
def image_build(
    file: str,
    build_arg: Sequence[str],
    path: str,
    image_uri: str,
    volume: Sequence[str],
    env: Sequence[str],
) -> None:
    run_async(_build_image(file, path, image_uri, build_arg, volume, env))


@image.command("copy")
@click.argument("source")
@click.argument("destination")
def image_copy(source: str, destination: str) -> None:
    run_async(_copy_image(source, destination))


@main.command("cp")
@click.argument("source")
@click.argument("destination")
def cluster_copy(source: str, destination: str) -> None:
    run_async(_copy_storage(source, destination))


async def _copy_storage(source: str, destination: str) -> None:
    src_uri = uri_from_cli(source, "", "")
    src_cluster = src_uri.host
    src_path = src_uri.parts[2:]

    dst_uri = uri_from_cli(destination, "", "")
    dst_cluster = dst_uri.host
    dst_path = dst_uri.parts[2:]

    assert src_cluster
    assert dst_cluster
    async with neuro_api.get() as client:
        await client.config.switch_cluster(dst_cluster)
        await client.storage.mkdir(URL("storage:"), parents=True, exist_ok=True)
    await _run_copy_container(src_cluster, "/".join(src_path), "/".join(dst_path))


async def _run_copy_container(src_cluster: str, src_path: str, dst_path: str) -> None:
    args = [
        "neuro",
        "run",
        "-s",
        "cpu-small",
        "--pass-config",
        "-v",
        "storage:://storage",
        "-e",
        f"NEURO_CLUSTER={src_cluster}",
        "neuromation/neuro-extras:latest",
        f'"cp --progress -r -u -T storage:{src_path} /storage/{dst_path}"',
    ]
    cmd = " ".join(args)
    print(f"Executing '{cmd}'")
    subprocess = await asyncio.create_subprocess_shell(cmd)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise Exception("Unable to copy storage")


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
        await _build_image("Dockerfile", tmpdir, destination, [], [], [])


async def _build_image(
    dockerfile_path: str,
    context: str,
    image_uri: str,
    build_args: Sequence[str],
    volume: Sequence[str],
    env: Sequence[str],
) -> None:
    async with neuro_api.get() as client:
        context_uri = uri_from_cli(
            context,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        builder = ImageBuilder(client)
        job = await builder.launch(
            dockerfile_path, context_uri, image_uri, build_args, volume, env
        )
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


@main.command("upload")
@click.argument("path")
def upload(path: str) -> None:
    """
    Upload neuro project files to storage

    Uploads file (or files under) project-root/PATH to
    storage://remote-project-dir/PATH. You can use "." for PATH to upload
    whole project. The "remote-project-dir" is set using .neuro.toml config,
    as in example:

    \b
    [extra]
    remote-project-dir = "project-dir-name"
    """
    return_code = run_async(_upload(path))
    exit(return_code)


@main.command("download")
@click.argument("path")
def download(path: str) -> None:
    """
    Download neuro project files from storage

    Downloads file (or files under) from storage://remote-project-dir/PATH
    to project-root/PATH. You can use "." for PATH to download whole project.
    The "remote-project-dir" is set using .neuro.toml config, as in example:

    \b
    [extra]
    remote-project-dir = "project-dir-name"
    """
    return_code = run_async(_download(path))
    exit(return_code)


def _get_project_root() -> Path:
    try:
        return find_project_root()
    except ConfigError:
        raise click.ClickException(
            "Not a Neu.ro project directory (or any of the parent directories)."
        )


async def _get_remote_project_root() -> Path:
    config = await load_user_config(Path("~/.neuro"))
    try:
        return Path(config["extra"]["remote-project-dir"])
    except KeyError:
        raise click.ClickException(
            '"remote-project-dir" configuration variable is not set. Please add'
            ' it to "extra" section of project config file.'
        )


async def _ensure_folder_exists(path: Path, remote: bool = False) -> None:
    if remote:
        subprocess = await asyncio.create_subprocess_exec(
            "neuro", "mkdir", "-p", f"storage:{path}"
        )
        returncode = await subprocess.wait()
        if returncode != 0:
            raise click.ClickException("Was unable to create containing directory")
    else:
        path.mkdir(parents=True, exist_ok=True)


async def _upload(path: str) -> int:
    target = _get_project_root() / path
    if not target.exists():
        raise click.ClickException(f"Folder or file does not exist: {target}")
    remote_project_root = await _get_remote_project_root()
    await _ensure_folder_exists((remote_project_root / path).parent, True)
    if target.is_dir():
        subprocess = await asyncio.create_subprocess_exec(
            "neuro",
            "cp",
            "--recursive",
            "-u",
            str(target),
            "-T",
            f"storage:{remote_project_root / path}",
        )
    else:
        subprocess = await asyncio.create_subprocess_exec(
            "neuro", "cp", str(target), f"storage:{remote_project_root / path}"
        )
    return await subprocess.wait()


async def _download(path: str) -> int:
    project_root = _get_project_root()
    remote_project_root = await _get_remote_project_root()
    await _ensure_folder_exists((project_root / path).parent, False)
    subprocess = await asyncio.create_subprocess_exec(
        "neuro",
        "cp",
        "--recursive",
        "-u",
        f"storage:{remote_project_root / path}",
        "-T",
        str(project_root / path),
    )
    return await subprocess.wait()


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
            "-e, --env environment variables for container",
            "-v, --volume list of volumes for container",
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
    config["alias"]["storage-cp"] = {
        "exec": "neuro-extras cp",
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
