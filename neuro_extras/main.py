import asyncio
import base64
import json
import logging
import os
import re
import sys
import tempfile
import textwrap
import uuid
from dataclasses import dataclass, field
from distutils import dir_util
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, MutableMapping, Optional, Sequence

import click
import toml
import yaml
from neuromation import api as neuro_api
from neuromation.api import ConfigError, Preset, Resources, find_project_root
from neuromation.api.config import load_user_config
from neuromation.api.parsing_utils import _as_repo_str
from neuromation.api.url_utils import normalize_storage_path_uri, uri_from_cli
from neuromation.cli.asyncio_utils import run as run_async
from neuromation.cli.click_types import PresetType
from neuromation.cli.const import EX_OK, EX_PLATFORMERROR
from yarl import URL

from neuro_extras.utils import get_neuro_client

from .version import __version__


NEURO_EXTRAS_IMAGE = os.environ.get(
    "NEURO_EXTRAS_IMAGE", f"neuromation/neuro-extras:{__version__}"
)

SUPPORTED_ARCHIVE_TYPES = (
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz2",
    ".tbz",
    ".tar",
    ".gz",
    ".zip",
)

SUPPORTED_OBJECT_STORAGE_SCHEMES = {
    "AWS": "s3://",
    "GCS": "gs://",
    "AZURE": "azure+https://",
    "HTTP": "http://",
    "HTTPS": "https://",
}


@click.group()
@click.version_option(
    version=__version__, message="neuro-extras package version: %(version)s"
)
def main() -> None:
    """
    Auxiliary scripts and recipes for automating routine tasks.
    """
    handler = ClickLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


@main.group()
def data() -> None:
    """
    Data transfer operations.
    """
    pass


@data.command("transfer")
@click.argument("source")
@click.argument("destination")
def data_transfer(source: str, destination: str) -> None:
    """
    Copy data between storages on different clusters.
    """
    run_async(_data_transfer(source, destination))


@main.group()
def image() -> None:
    """
    Job container image operations.
    """
    pass


@image.command("transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "-F",
    "--force-overwrite",
    default=False,
    is_flag=True,
    help="Transfer even if the destination image already exists.",
)
def image_transfer(source: str, destination: str, force_overwrite: bool) -> None:
    """
    Copy images between clusters.
    """
    exit_code = run_async(_image_transfer(source, destination, force_overwrite))
    sys.exit(exit_code)


@main.command("init-aliases")
def init_aliases() -> None:
    """
    Create neuro CLI aliases for neuro-extras functionality.
    """
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
            "-f, --file=PATH  path to the Dockerfile within CONTEXT",
            "--build-arg=LIST  build arguments for Docker",
            "-e, --env=LIST  environment variables for container",
            "-v, --volume=LIST  list of volumes for container",
            "-s, --preset=STR  specify  preset for builder container",
            "-F, --force-overwrite  enforce destination image overwrite",
        ],
        "args": "CONTEXT IMAGE_URI",
        "help": (
            "Build docker image on the platform. "
            "Hit `neuro-extras image build --help` for more info."
        ),
    }
    config["alias"]["seldon-init-package"] = {
        "exec": "neuro-extras seldon init-package",
        "args": "URI_OR_PATH",
    }
    config["alias"]["image-transfer"] = {
        "exec": "neuro-extras image transfer",
        "args": "SOURCE DESTINATION",
        "options": [
            "-F, --force-overwrite  enforce destination image overwrite",
        ],
        "help": (
            "Transfer images between the cluster within the platform. "
            "Hit `neuro-extras image transfer --help` for more info."
        ),
    }
    config["alias"]["data-transfer"] = {
        "exec": "neuro-extras data transfer",
        "args": "SOURCE DESTINATION",
    }
    config["alias"]["data-cp"] = {
        "exec": "neuro-extras data cp",
        "options": [
            "-c, --compress Compress source files",
            "-x, --extract Extract downloaded files",
            "-e, --env environment variables for container",
            "-v, --volume list of volumes for container",
            "-t, --use-temp-dir store intermediate data in TMP directory",
        ],
        "args": "SOURCE DESTINATION",
    }
    with toml_path.open("w") as f:
        toml.dump(config, f)
    logger.info(f"Added aliases to {toml_path}")


class ClickLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            click.echo(msg)
        except Exception:
            self.handleError(record)


async def _save_docker_json(path: str) -> None:
    async with get_neuro_client() as client:
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


@main.group()
def config() -> None:
    """
    Configuration operations.
    """
    pass


@config.command(
    "save-docker-json",
    help="Generate JSON configuration file for accessing cluster registry.",
)
@click.argument("path")
def config_save_docker_json(path: str) -> None:
    run_async(_save_docker_json(path))


TEMP_UNPACK_DIR = Path.home() / ".neuro-tmp"


async def _parse_neuro_image(image: str) -> neuro_api.RemoteImage:
    async with get_neuro_client() as client:
        return client.parse.remote_image(image)


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
        use_temp_dir: bool,
    ) -> neuro_api.JobDescription:
        logger.info("Submitting a copy job")
        copier_container = await self._create_copier_container(
            extract, src_uri, dst_uri, volume, env, use_temp_dir
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
        use_temp_dir: bool,
    ) -> neuro_api.Container:
        args = f"{str(src_uri)} {str(dst_uri)}"
        if extract:
            args = f"-x {args}"
        if use_temp_dir:
            args = f"--use-temp-dir {args}"

        env_parse_result = self._client.parse.envs(env)
        vol = self._client.parse.volumes(volume)
        volumes, secret_files, disk_volumes = (
            list(vol.volumes),
            list(vol.secret_files),
            list(vol.disk_volumes),
        )

        cmd = f"neuro-extras data cp {args}"
        image = await _parse_neuro_image(NEURO_EXTRAS_IMAGE)
        return neuro_api.Container(
            image=image,
            resources=neuro_api.Resources(cpu=2.0, memory_mb=4096),
            volumes=volumes,
            disk_volumes=disk_volumes,
            command=f"bash -c '{cmd} '",
            env=env_parse_result.env,
            secret_env=env_parse_result.secret_env,
            secret_files=secret_files,
        )


class UrlType(Enum):
    UNSUPPORTED = 0
    LOCAL = 1
    CLOUD = 2
    STORAGE = 3
    DISK = 4

    @staticmethod
    def get_type(url: URL) -> "UrlType":
        if url.scheme == "storage":
            return UrlType.STORAGE
        if url.scheme == "":
            return UrlType.LOCAL
        if url.scheme in ("s3", "gs", "azure+https", "http", "https"):
            return UrlType.CLOUD
        if url.scheme == "disk":
            return UrlType.DISK
        return UrlType.UNSUPPORTED


async def _data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: List[str],
    env: List[str],
    use_temp_dir: bool,
) -> None:
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
    if source_url_type == UrlType.DISK and destination_url_type == UrlType.DISK:
        raise ValueError(
            "This command can't be used to copy data between two persistent disks"
        )
    if destination_url.scheme in ("http", "https"):
        raise ValueError("This command can't be used to upload data over HTTP(S)")

    # Persistent disk and storage locations must be mounted as folders to a job
    if UrlType.STORAGE in (source_url_type, destination_url_type) or UrlType.DISK in (
        source_url_type,
        destination_url_type,
    ):
        if source_url_type == UrlType.STORAGE:
            volume.append(f"{str(source_url)}:/var/storage")
            container_src_uri = URL("/var/storage")
        elif source_url_type == UrlType.DISK:
            volume.append(f"{str(source_url)}:/var/disk:rw")
            container_src_uri = URL("/var/disk/")
        else:
            container_src_uri = source_url

        if destination_url_type == UrlType.STORAGE:
            volume.append(f"{str(destination_url.parent)}:/var/storage")
            container_dst_uri = URL(f"/var/storage/{destination_url.name}")
        elif destination_url_type == UrlType.DISK:
            disk_id = str(destination_url.path)[:41]  # disk ID is 41 symbols long
            volume.append(f"disk:{disk_id}:/var/disk:rw")
            container_dst_uri = URL(f"/var/disk/{str(destination_url.path)[42:]}")
        else:
            container_dst_uri = destination_url

        async with get_neuro_client() as client:
            data_copier = DataCopier(client)
            job = await data_copier.launch(
                src_uri=container_src_uri,
                dst_uri=container_dst_uri,
                extract=extract,
                volume=volume,
                env=env,
                use_temp_dir=use_temp_dir,
            )
            exit_code = await _attach_job_stdout(job, client, name="copy")
            if exit_code == EX_OK:
                logger.info("Successfully copied data")
            else:
                raise click.ClickException(f"Data copy failed: {exit_code}")

    else:
        # otherwise we deal with cloud/local src and destination
        origin_src_url = source_url
        dst_dir = Path(destination_url.path).parent
        dst_name = origin_dst_name = Path(destination_url.path).name

        if use_temp_dir:
            # substitute original dst with TMP dst.
            # do extraction / compression there
            TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
            dst_dir = Path(tempfile.mkdtemp(dir=str(TEMP_UNPACK_DIR)))
        if extract:
            # download data into subfolder of target for extraction
            dst_name = origin_dst_name + "/" + origin_src_url.name
        if compress:
            # preserve origin destination name for compressor
            if origin_dst_name == origin_src_url.name:
                logging.warning(
                    "Source file already has required archive extension. "
                    "Skipping compression step."
                )
                compress = False
            else:
                dst_name = origin_src_url.name

        cp_destination_url = URL.build(path=str(dst_dir / dst_name))
        await _nonstorage_cp(origin_src_url, cp_destination_url, remove_source=False)
        source_url = cp_destination_url

        if extract:
            dir_util.mkpath(str(dst_dir))
            extraction_dst_url = URL.build(path=str(dst_dir / origin_dst_name))
            await _extract(source_url, extraction_dst_url, rm_src=True)
            source_url = extraction_dst_url
        if compress:
            compression_dst_url = URL.build(path=str(dst_dir / origin_dst_name))
            await _compress(source_url, compression_dst_url, rm_src=True)
            source_url = compression_dst_url

        if use_temp_dir:
            # Move downloaded and maybe extracted / compressed files to
            # original destination.
            # Otherwise (if tmp was not used) - they are already there.
            await _nonstorage_cp(source_url, destination_url, remove_source=True)


def _patch_azure_url_for_rclone(url: URL) -> str:
    if url.scheme == "azure+https":
        return f":azureblob:{url.path}"
    else:
        return str(url)


def _build_sas_url(source_url: URL, destination_url: URL) -> URL:
    """
    In order to build SAS URL we replace original URL scheme with HTTPS,
    remove everything from path except bucket name and append SAS token
    """
    azure_url = source_url if source_url.scheme == "azure+https" else destination_url
    sas_token = os.getenv("AZURE_SAS_TOKEN")
    azure_url = (
        azure_url.with_scheme("https")
        .with_path("/".join(azure_url.path.split("/")[:2]))
        .with_query(sas_token)
    )
    return azure_url


async def _nonstorage_cp(
    source_url: URL, destination_url: URL, remove_source: bool = False
) -> None:
    if "s3" in (source_url.scheme, destination_url.scheme):
        command = "aws"
        args = ["s3", "cp", str(source_url), str(destination_url)]
        if source_url.path.endswith("/"):
            args.insert(2, "--recursive")
    elif "gs" in (source_url.scheme, destination_url.scheme):
        command = "gsutil"
        # gsutil service credentials are activated in entrypoint.sh
        args = ["-m", "cp", "-r", str(source_url), str(destination_url)]
    elif "azure+https" in (source_url.scheme, destination_url.scheme):
        sas_url = _build_sas_url(source_url, destination_url)
        command = "rclone"
        args = [
            "copyto",
            "--azureblob-sas-url",
            str(sas_url),
            _patch_azure_url_for_rclone(source_url),
            _patch_azure_url_for_rclone(destination_url),
        ]
    elif source_url.scheme in ("http", "https"):
        command = "rclone"
        args = [
            "copyto",
            "--http-url",
            # HTTP URL parameter for rclone is just scheme + host name
            str(source_url.with_path("").with_query("")),
            f":http:{source_url.path}",
            str(destination_url),
        ]
    elif source_url.scheme == "" and destination_url.scheme == "":
        command = "rclone"
        args = [
            "copyto",  # TODO: investigate usage of 'sync' for potential speedup.
            "--checkers=16",  # https://rclone.org/docs/#checkers-n , default is 8
            "--transfers=8",  # https://rclone.org/docs/#transfers-n , default is 4.
            "--verbose=1",  # default is 0, set 2 for debug
            str(source_url),
            str(destination_url),
        ]
    else:
        raise ValueError("Unknown cloud provider")
    click.echo(f"Running '{command} {' '.join(args)}'")
    subprocess = await asyncio.create_subprocess_exec(command, *args)
    returncode = await subprocess.wait()
    if UrlType.get_type(source_url) == UrlType.LOCAL and remove_source:
        _rm_local(Path(source_url.path))
    if returncode != 0:
        raise click.ClickException("Cloud copy failed")


async def _extract(source_url: URL, destination_url: URL, rm_src: bool) -> None:
    file = Path(source_url.path)
    if file.is_dir():
        file = list(file.glob("*"))[0]
    suffixes = file.suffixes
    if suffixes[-2:] == [".tar", ".gz"] or suffixes[-1] == ".tgz":
        command = "tar"
        args = ["zxvf", str(file), "-C", str(destination_url.path)]
    elif suffixes[-2:] == [".tar", ".bz2"] or suffixes[-1] in (".tbz2", ".tbz"):
        command = "tar"
        args = ["jxvf", str(file), "-C", str(destination_url)]
    elif suffixes[-1] == ".tar":
        command = "tar"
        args = ["xvf", str(file), "-C", str(destination_url)]
    elif suffixes[-1] == ".gz":
        command = "gunzip"
        args = ["--keep", str(file), str(destination_url) + file.name[:-3]]
    elif suffixes[-1] == ".zip":
        command = "unzip"
        args = [str(file), "-d", str(destination_url)]
    else:
        raise ValueError(
            f"Don't know how to extract file {file.name}"
            f"Supported archive types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}"
        )

    click.echo(f"Running '{command} {' '.join(args)}'")
    subprocess = await asyncio.create_subprocess_exec(command, *args)
    returncode = await subprocess.wait()
    if rm_src:
        _rm_local(Path(source_url.path))
    if returncode != 0:
        raise click.ClickException(f"Extraction failed: {subprocess.stderr}")


async def _compress(source_url: URL, destination_url: URL, rm_src: bool) -> None:
    file = Path(destination_url.path.split("/")[-1])
    suffixes = file.suffixes
    if suffixes[-2:] == [".tar", ".gz"] or suffixes[-1] == ".tgz":
        command = "tar"
        args = [
            "zcf",
            str(destination_url.path),
            "-C",
            str(Path(source_url.path).parent),
            f"--exclude={str(destination_url.name)}",
            ".",
        ]
    elif suffixes[-2:] == [".tar", ".bz2"] or suffixes[-1] in (".tbz2", ".tbz"):
        command = "tar"
        args = [
            "jcf",
            str(destination_url.path),
            f"--exclude={str(destination_url.name)}",
            str(source_url.path),
        ]
    elif suffixes[-1] == ".tar":
        command = "tar"
        args = [
            "cf",
            str(destination_url.path),
            f"--exclude={str(destination_url.name)}",
            str(source_url.path),
        ]
    elif suffixes[-1] == ".gz":
        command = "gzip"
        args = ["-r", str(destination_url.path), str(source_url.path)]
    elif suffixes[-1] == ".zip":
        command = "zip"
        args = ["-r", str(destination_url.path), str(source_url.path)]
    else:
        raise ValueError(
            f"Don't know how to compress to archive type {file.name}. "
            f"Supported archive types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}"
        )

    click.echo(f"Running '{command} {' '.join(args)}'")
    subprocess = await asyncio.create_subprocess_exec(command, *args)
    returncode = await subprocess.wait()
    if rm_src:
        _rm_local(Path(source_url.path))
    if returncode != 0:
        raise click.ClickException(f"Compression failed: {subprocess.stderr}")


def _rm_local(target: Path) -> None:
    # maybe also AWS / GCS clouds or storage?
    if target.is_dir():
        dir_util.remove_tree(str(target))
    if target.is_file() and target.exists():
        target.unlink()


@data.command(
    "cp",
    help=(
        "Copy data between external object storage and cluster. "
        "Supported external object storage systems: "
        f"{set(SUPPORTED_OBJECT_STORAGE_SCHEMES.keys())}"
    ),
)
@click.argument("source")
@click.argument("destination")
@click.option(
    "-x",
    "--extract",
    default=False,
    is_flag=True,
    help=(
        "Perform extraction of SOURCE into the DESTINATION directory. "
        "The archive type is derived from the file name. "
        f"Supported types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}."
    ),
)
@click.option(
    "-c",
    "--compress",
    default=False,
    is_flag=True,
    help=(
        "Perform compression of SOURCE into the DESTINATION file. "
        "The archive type is derived from the file name. "
        f"Supported types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}."
    ),
)
@click.option(
    "-v",
    "--volume",
    metavar="MOUNT",
    multiple=True,
    help=(
        "Mounts directory from vault into container. "
        "Use multiple options to mount more than one volume."
    ),
)
@click.option(
    "-e",
    "--env",
    metavar="VAR=VAL",
    multiple=True,
    help=(
        "Set environment variable in container. "
        "Use multiple options to define more than one variable."
    ),
)
@click.option(
    "-t",
    "--use-temp-dir",
    default=False,
    is_flag=True,
    help=(
        "Download and extract / compress data (if needed) "
        " inside the temporal directory. "
        "Afterwards move resulted file(s) into the DESTINATION. "
        "NOTE: use it if 'storage:' is involved and "
        "extraction or compression is performed to speedup the process."
    ),
)
def data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: Sequence[str],
    env: Sequence[str],
    use_temp_dir: bool,
) -> None:
    if extract and compress:
        raise click.ClickException("Extract and compress can't be used together")
    run_async(
        _data_cp(
            source,
            destination,
            extract,
            compress,
            list(volume),
            list(env),
            use_temp_dir,
        )
    )


def _get_cluster_from_uri(image_uri: str, *, scheme: str) -> Optional[str]:
    uri = uri_from_cli(image_uri, "", "", allowed_schemes=[scheme])
    return uri.host


async def _image_transfer(src_uri: str, dst_uri: str, force_overwrite: bool) -> int:
    src_cluster: Optional[str] = _get_cluster_from_uri(src_uri, scheme="image")
    dst_cluster: Optional[str] = _get_cluster_from_uri(dst_uri, scheme="image")
    if not dst_cluster:
        raise ValueError(f"Invalid destination image {dst_uri}: missing cluster name")

    with tempfile.TemporaryDirectory() as tmpdir:
        async with get_neuro_client(cluster=src_cluster) as src_client:
            src_image = src_client.parse.remote_image(image=src_uri)
            src_client_config = src_client.config

        dockerfile = Path(f"{tmpdir}/Dockerfile")
        dockerfile.write_text(
            textwrap.dedent(
                f"""\
                FROM {_as_repo_str(src_image)}
                LABEL neu.ro/source-image-uri={src_uri}
                """
            )
        )
        return await _build_image(
            dockerfile_path=dockerfile.name,
            context=tmpdir,
            image_uri=dst_uri,
            use_cache=True,
            build_args=[],
            volume=[],
            env=[],
            force_overwrite=force_overwrite,
            other_client_configs=[src_client_config],
        )


async def _attach_job_stdout(
    job: neuro_api.JobDescription, client: neuro_api.Client, name: str = ""
) -> int:
    while job.status == neuro_api.JobStatus.PENDING:
        job = await client.jobs.status(job.id)
        await asyncio.sleep(1.0)
    async for chunk in client.jobs.monitor(job.id):
        if not chunk:
            break
        click.echo(chunk.decode(errors="ignore"), nl=False)
    while job.status in (neuro_api.JobStatus.PENDING, neuro_api.JobStatus.RUNNING):
        job = await client.jobs.status(job.id)
        await asyncio.sleep(1.0)

    job = await client.jobs.status(job.id)
    exit_code = EX_PLATFORMERROR
    if job.status == neuro_api.JobStatus.SUCCEEDED:
        exit_code = EX_OK
    elif job.status == neuro_api.JobStatus.FAILED:
        logger.error(f"The {name} job {job.id} failed due to:")
        logger.error(f"  Reason: {job.history.reason}")
        logger.error(f"  Description: {job.history.description}")
        exit_code = job.history.exit_code or EX_PLATFORMERROR  # never 0 for failed
    elif job.status == neuro_api.JobStatus.CANCELLED:
        logger.error(f"The {name} job {job.id} was cancelled")
    else:
        logger.error(f"The {name} job {job.id} terminated, status: {job.status}")
    return exit_code


async def _build_image(
    dockerfile_path: str,
    context: str,
    image_uri: str,
    use_cache: bool,
    build_args: Sequence[str],
    volume: Sequence[str],
    env: Sequence[str],
    force_overwrite: bool,
    preset: Optional[str] = None,
    other_client_configs: Sequence[neuro_api.Config] = (),
    verbose: bool = False,
) -> int:
    cluster = _get_cluster_from_uri(image_uri, scheme="image")
    async with get_neuro_client(cluster=cluster) as client:
        if not preset:
            preset = next(iter(client.config.presets.keys()))
        job_preset = client.config.presets[preset]
        context_uri = uri_from_cli(
            context,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        target_image = await _parse_neuro_image(image_uri)
        try:
            existing_images = await client.images.tags(
                neuro_api.RemoteImage(
                    name=target_image.name,
                    owner=target_image.owner,
                    cluster_name=target_image.cluster_name,
                    registry=target_image.registry,
                    tag=None,
                )
            )
        except neuro_api.errors.ResourceNotFound:
            # target_image does not exists on platform registry, skip else block
            pass
        else:
            if target_image in existing_images and force_overwrite:
                logger.warning(
                    f"Target image '{target_image}' exists and will be overwritten."
                )
            elif target_image in existing_images and not force_overwrite:
                raise click.ClickException(
                    f"Target image '{target_image}' exists. "
                    f"Use -F/--force-overwrite flag to enforce overwriting."
                )

        builder = ImageBuilder(
            client, other_clients_configs=other_client_configs, verbose=verbose
        )
        job = await builder.launch(
            dockerfile_path=dockerfile_path,
            context_uri=context_uri,
            image_uri_str=image_uri,
            use_cache=use_cache,
            build_args=build_args,
            volume=volume,
            env=env,
            job_preset=job_preset,
        )
        exit_code = await _attach_job_stdout(job, client, name="builder")
        if exit_code == EX_OK:
            logger.info(f"Successfully built {image_uri}")
            return EX_OK
        else:
            raise click.ClickException(f"Failed to build image: {exit_code}")


PRESET = PresetType()


@image.command(
    "build", help="Build Job container image remotely on cluster using Kaniko."
)
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
@click.option(
    "-s",
    "--preset",
    metavar="PRESET",
    help=(
        "Predefined resource configuration (to see available values, "
        "run `neuro config show`)"
    ),
)
@click.option(
    "-F",
    "--force-overwrite",
    default=False,
    is_flag=True,
    help="Build even if the destination image already exists.",
)
@click.option(
    "--cache/--no-cache",
    default=True,
    show_default=True,
    help="Use kaniko cache while building image",
)
@click.argument("path")
@click.argument("image_uri")
@click.option("--verbose", type=bool, default=False)
def image_build(
    file: str,
    build_arg: Sequence[str],
    path: str,
    image_uri: str,
    volume: Sequence[str],
    env: Sequence[str],
    preset: str,
    force_overwrite: bool,
    cache: bool,
    verbose: bool,
) -> None:
    try:
        sys.exit(
            run_async(
                _build_image(
                    dockerfile_path=file,
                    context=path,
                    image_uri=image_uri,
                    use_cache=cache,
                    build_args=build_arg,
                    volume=volume,
                    env=env,
                    preset=preset,
                    force_overwrite=force_overwrite,
                    verbose=verbose,
                )
            )
        )
    except (ValueError, click.ClickException) as e:
        logger.error(f"Failed to build image: {e}")
        sys.exit(EX_PLATFORMERROR)


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


async def _create_k8s_secret(name: str) -> Dict[str, Any]:
    async with get_neuro_client() as client:
        payload: Dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name},
            "type": "Opaque",
            "data": {},
        }
        config_path = Path(client.config._path)
        for path in config_path.iterdir():
            if path.is_dir() or path.name in ("db-shm", "db-wal"):
                continue
            payload["data"][path.name] = base64.b64encode(path.read_bytes()).decode()
        return payload


async def _create_k8s_registry_secret(name: str) -> Dict[str, Any]:
    async with get_neuro_client() as client:
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


@main.group()
def k8s() -> None:
    """
    Cluster Kubernetes operations.
    """
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


logger = logging.getLogger(__name__)


async def _init_seldon_package(path: str) -> None:
    async with get_neuro_client() as client:
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


async def _create_seldon_deployment(
    *,
    name: str,
    neuro_secret_name: str,
    registry_secret_name: str,
    model_image_uri: str,
    model_storage_uri: str,
) -> Dict[str, Any]:
    async with get_neuro_client() as client:
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
                "image": NEURO_EXTRAS_IMAGE,
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


ASSETS_PATH = Path(__file__).resolve().parent / "assets"
SELDON_CUSTOM_PATH = ASSETS_PATH / "seldon.package"


@main.group()
def seldon() -> None:
    """
    Seldon deployment operations.
    """
    pass


@seldon.command("init-package")
@click.argument("path", default=".")
def seldon_init_package(path: str) -> None:
    run_async(_init_seldon_package(path))


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


async def _data_transfer(src_uri: str, dst_uri: str) -> None:
    src_cluster_or_null = _get_cluster_from_uri(src_uri, scheme="storage")
    dst_cluster = _get_cluster_from_uri(dst_uri, scheme="storage")

    if not src_cluster_or_null:
        async with get_neuro_client() as src_client:
            src_cluster = src_client.cluster_name
    else:
        src_cluster = src_cluster_or_null

    if not dst_cluster:
        raise ValueError(f"Invalid destination path {dst_uri}: missing cluster name")

    async with get_neuro_client(cluster=dst_cluster) as client:
        await client.storage.mkdir(URL("storage:"), parents=True, exist_ok=True)
        await _run_copy_container(src_cluster, src_uri, dst_uri)


async def _run_copy_container(src_cluster: str, src_uri: str, dst_uri: str) -> None:
    args = [
        "neuro",
        "run",
        "-s",
        "cpu-small",
        "--pass-config",
        "-v",
        f"{dst_uri}:/storage:rw",
        "-e",
        f"NEURO_CLUSTER={src_cluster}",  # inside the job, switch neuro to 'src_cluster'
        NEURO_EXTRAS_IMAGE,
        f"neuro cp --progress -r -u -T {src_uri} /storage",
    ]
    cmd = " ".join(args)
    click.echo(f"Running '{cmd}'")
    subprocess = await asyncio.create_subprocess_shell(cmd)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise click.ClickException("Unable to copy storage")


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


def _get_project_root() -> Path:
    try:
        return find_project_root()
    except ConfigError:
        raise click.ClickException(
            "Not a Neu.ro project directory (or any of the parent directories)."
        )


async def _get_remote_project_root() -> Path:
    config = load_user_config(Path("~/.neuro"))
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


@main.command("upload")
@click.argument("path")
def upload(path: str) -> None:
    """
    Upload neuro project files to storage.

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
    Download neuro project files from storage.

    Downloads file (or files under) from storage://remote-project-dir/PATH
    to project-root/PATH. You can use "." for PATH to download whole project.
    The "remote-project-dir" is set using .neuro.toml config, as in example:

    \b
    [extra]
    remote-project-dir = "project-dir-name"
    """
    return_code = run_async(_download(path))
    exit(return_code)
