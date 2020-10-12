import asyncio
import logging
import sys
import tempfile
from distutils import dir_util
from enum import Enum
from pathlib import Path
from typing import List, Sequence

import click
from neuromation import api as neuro_api
from neuromation.api.url_utils import uri_from_cli
from neuromation.cli.asyncio_utils import run as run_async
from neuromation.cli.const import EX_PLATFORMERROR
from yarl import URL

from .cli import main
from .data_copier import NEURO_EXTRAS_IMAGE, DataCopier


logger = logging.getLogger(__name__)

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
TEMP_UNPACK_DIR = Path.home() / ".neuro-tmp"


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
        if url.scheme in ("s3", "gs"):
            return UrlType.CLOUD
        if url.scheme == "disk":
            return UrlType.DISK
        return UrlType.UNSUPPORTED


@main.group()
def data() -> None:
    pass


@data.command("transfer")
@click.argument("source")
@click.argument("destination")
def data_transfer(source: str, destination: str) -> None:
    run_async(_transfer_data(source, destination))


@data.command("cp")
@click.argument("source")
@click.argument("destination")
@click.option(
    "-x",
    "--extract",
    default=False,
    is_flag=True,
    help=(
        "Perform extraction of SOURCE into the temporal folder and move "
        "extracted files to DESTINATION. The archive type is derived "
        "from the file name. "
        f"Supported types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}."
    ),
)
@click.option(
    "-c",
    "--compress",
    default=False,
    is_flag=True,
    help=(
        "Perform compression of SOURCE into the temporal folder and move "
        "created archive to DESTINATION. The archive type is derived "
        "from the file name. "
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
        "Use multiple options to mount more than one volume. "
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
def data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: Sequence[str],
    env: Sequence[str],
) -> None:
    if extract and compress:
        raise click.ClickException("Extract and compress can't be used together")
    run_async(_data_cp(source, destination, extract, compress, list(volume), list(env)))


async def _data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: List[str],
    env: List[str],
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
            volume.append(f"{str(destination_url)}:/var/storage")
            container_dst_uri = URL("/var/storage")
        elif destination_url_type == UrlType.DISK:
            volume.append(f"{str(destination_url)}:/var/disk:rw")
            container_dst_uri = URL("/var/disk/")
        else:
            container_dst_uri = destination_url

        async with neuro_api.get() as client:
            data_copier = DataCopier(client)
            job = await data_copier.launch(
                src_uri=container_src_uri,
                dst_uri=container_dst_uri,
                extract=extract,
                volume=volume,
                env=env,
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
        # otherwise we deal with cloud/local src and destination

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
                raise ValueError(
                    f"Don't know how to extract file {file.name}"
                    f"Supported archive types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}"
                )

            click.echo(f"Running {command} {' '.join(args)}")
            subprocess = await asyncio.create_subprocess_exec(command, *args)
            returncode = await subprocess.wait()
            if returncode != 0:
                raise click.ClickException(f"Extraction failed: {subprocess.stderr}")
            else:
                if file.exists():
                    # gunzip removes src after extraction, while tar - not
                    file.unlink()

        if compress:
            file = Path(destination_url.path.split("/")[-1])
            tmp_dst_archive = Path(TEMP_UNPACK_DIR / file)
            suffixes = file.suffixes
            if suffixes[-2:] == [".tar", ".gz"] or suffixes[-1] == ".tgz":
                command = "tar"
                args = ["zcf", str(tmp_dst_archive), "-C", str(source_url.path), "."]
            elif suffixes[-2:] == [".tar", ".bz2"] or suffixes[-1] in (".tbz2", ".tbz"):
                command = "tar"
                args = ["jcf", str(tmp_dst_archive), str(source_url.path)]
            elif suffixes[-1] == ".tar":
                command = "tar"
                args = ["cf", str(tmp_dst_archive), str(source_url.path)]
            elif suffixes[-1] == ".gz":
                command = "gzip"
                args = ["-r", str(tmp_dst_archive), str(source_url.path)]
            elif suffixes[-1] == ".zip":
                command = "zip"
                args = [str(tmp_dst_archive), str(source_url.path)]
            else:
                raise ValueError(
                    f"Don't know how to compress to archive type {file.name}. "
                    f"Supported archive types: {', '.join(SUPPORTED_ARCHIVE_TYPES)}"
                )

            click.echo(f"Running {command} {' '.join(args)}")
            subprocess = await asyncio.create_subprocess_exec(command, *args)
            returncode = await subprocess.wait()
            if returncode != 0:
                raise click.ClickException(f"Compression failed: {subprocess.stderr}")
            else:
                source_url = URL(str(tmp_dst_archive))
                # At this moment we know destination URL is a file, but we need its
                # parent directory
                destination_file = Path(destination_url.path)
                destination_dir = destination_file.parent
                destination_url = URL.build(
                    scheme=destination_url.scheme, path=str(destination_dir)
                )

        # handle upload/rclone
        await _nonstorage_cp(source_url, destination_url, remove_source=True)
        if compress:
            if tmp_dst_archive.exists():
                tmp_dst_archive.unlink()


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
        args = ["-m", "cp", "-r", str(source_url), str(destination_url)]
    elif source_url.scheme == "" and destination_url.scheme == "":
        command = "rclone"
        args = [
            "copy",  # TODO: investigate usage of 'sync' for potential speedup.
            "--checkers=16",  # https://rclone.org/docs/#checkers-n , default is 8
            "--transfers=8",  # https://rclone.org/docs/#transfers-n , default is 4.
            "--verbose=1",  # default is 0, set 2 for debug
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
        if remove_source:
            if source_path.is_dir():
                dir_util.remove_tree(str(source_path))
            else:
                if source_path.exists():
                    source_path.unlink()


async def _transfer_data(source: str, destination: str) -> None:
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
        NEURO_EXTRAS_IMAGE,
        f'"neuro cp --progress -r -u -T storage:{src_path} /storage/{dst_path}"',
    ]
    cmd = " ".join(args)
    print(f"Executing '{cmd}'")
    subprocess = await asyncio.create_subprocess_shell(cmd)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise Exception("Unable to copy storage")
