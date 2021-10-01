import asyncio
import logging
import os
import tempfile
from distutils import dir_util
from enum import Enum
from pathlib import Path
from typing import List, Optional, Sequence
from urllib import parse

import click
import neuro_sdk as neuro_api
from neuro_cli.asyncio_utils import run as run_async
from neuro_cli.const import EX_OK
from yarl import URL

from .cli import main
from .common import NEURO_EXTRAS_IMAGE, _attach_job_stdout
from .image import _get_cluster_from_uri, _parse_neuro_image
from .utils import get_neuro_client


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
    # originally, Azure's blob scheme is 'http(s)',
    # but we prepend 'azure+' to differenciate https vs azure
    "AZURE": "azure+https://",
    "HTTP": "http://",
    "HTTPS": "https://",
}

TEMP_UNPACK_DIR = Path.home() / ".neuro-tmp"

logger = logging.getLogger(__name__)


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
        preset: Optional[str] = None,
        life_span: Optional[int] = None,
    ) -> neuro_api.JobDescription:
        logger.info("Submitting a copy job")

        image = await _parse_neuro_image(NEURO_EXTRAS_IMAGE)

        command = self._build_command(dst_uri, src_uri, extract, use_temp_dir)

        env_parse_result = self._client.parse.envs(env)

        vol = self._client.parse.volumes(volume)
        volumes, secret_files, disk_volumes = (
            list(vol.volumes),
            list(vol.secret_files),
            list(vol.disk_volumes),
        )

        job = await self._client.jobs.start(
            image=image,
            preset_name=preset or list(self._client.presets.keys())[0],
            command=command,
            env=env_parse_result.env,
            secret_env=env_parse_result.secret_env,
            volumes=volumes,
            secret_files=secret_files,
            disk_volumes=disk_volumes,
            life_span=life_span or 60 * 60,
        )

        logger.info(f"The copy job ID: {job.id}")
        return job

    def _build_command(
        self, dst_uri: URL, src_uri: URL, extract: bool, use_temp_dir: bool
    ) -> str:
        args = f"{str(src_uri)} {str(dst_uri)}"
        if extract:
            args = f"-x {args}"
        if use_temp_dir:
            args = f"--use-temp-dir {args}"
        return f"neuro-extras data cp {args}"


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
    Copy data between storages on different clusters. \n
    Consider archiving dataset first for the sake of performance,
    if the dataset contains a lot (100k+) of small (< 100Kb each) files.
    """
    run_async(_data_transfer(source, destination))


@data.command(
    "cp",
    help=(
        "Copy data between external object storage and cluster. "
        "Supported external object storage systems: "
        f"{list(SUPPORTED_OBJECT_STORAGE_SCHEMES.keys())}. "
        "Note: originally, Azure's blob storage scheme is 'http(s)', "
        "but we prepend 'azure+' to differenciate https vs azure"
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
@click.option(
    "-s",
    "--preset",
    metavar="PRESET_NAME",
    help=("Preset name used for copy."),
)
@click.option(
    "-l",
    "--life_span",
    metavar="SECONDS",
    help=("Copy job life span in seconds."),
)
def data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: Sequence[str],
    env: Sequence[str],
    use_temp_dir: bool,
    preset: Optional[str] = None,
    life_span: Optional[int] = None,
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
            preset,
            life_span,
        )
    )


async def _data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: List[str],
    env: List[str],
    use_temp_dir: bool,
    preset: Optional[str] = None,
    life_span: Optional[int] = None,
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
                preset=preset,
                life_span=life_span,
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
    remove everything from path except bucket name and append SAS token as a query
    """
    azure_url = source_url if source_url.scheme == "azure+https" else destination_url
    sas_token = os.getenv("AZURE_SAS_TOKEN", "")
    azure_url = (
        azure_url.with_scheme("https")
        .with_path("/".join(azure_url.path.split("/")[:2]))
        .with_query(sas_token)
    )
    # with_query performs urlencode of sas_token, which brokes the token,
    # so we urldecode the resulting url
    azure_url = URL(parse.unquote(str(azure_url)))
    logger.info("Azure URL: %s", azure_url)
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


async def _data_transfer(src_uri_str: str, dst_uri_str: str) -> None:
    src_cluster_or_null = _get_cluster_from_uri(src_uri_str, scheme="storage")
    dst_cluster = _get_cluster_from_uri(dst_uri_str, scheme="storage")

    if not src_cluster_or_null:
        async with get_neuro_client() as src_client:
            src_cluster = src_client.cluster_name
    else:
        src_cluster = src_cluster_or_null

    if not dst_cluster:
        raise ValueError(
            f"Invalid destination path {dst_uri_str}: missing cluster name"
        )

    async with get_neuro_client(cluster=dst_cluster) as client:
        await client.storage.mkdir(URL("storage:"), parents=True, exist_ok=True)
        await _run_copy_container(src_cluster, src_uri_str, dst_uri_str)


async def _run_copy_container(
    src_cluster: str, src_uri_str: str, dst_uri_str: str
) -> None:
    args = [
        "neuro",
        "run",
        "-s",
        "cpu-small",
        "--pass-config",
        "-v",
        f"{dst_uri_str}:/storage:rw",
        "-e",
        f"NEURO_CLUSTER={src_cluster}",  # inside the job, switch neuro to 'src_cluster'
        "--life-span 10d",
        NEURO_EXTRAS_IMAGE,
        "--",
        f"neuro --show-traceback cp --progress -r -u -T {src_uri_str} /storage",
    ]
    cmd = " ".join(args)
    click.echo(f"Running '{cmd}'")
    subprocess = await asyncio.create_subprocess_shell(cmd)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise click.ClickException("Unable to copy storage")
