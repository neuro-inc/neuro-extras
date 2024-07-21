import asyncio
import logging
from pathlib import Path
from typing import Optional, Sequence

import click
from yarl import URL

from ..cli import main
from ..common import APOLO_EXTRAS_IMAGE
from ..image import _get_cluster_from_uri
from ..utils import get_platform_client
from .archive import ArchiveType
from .operations import CopyOperation


SUPPORTED_ARCHIVE_TYPES = list(ArchiveType.get_extension_mapping().keys())
SUPPORTED_OBJECT_STORAGE_SCHEMES = {
    "AWS": "s3://",
    "GCS": "gs://",
    # originally, Azure's blob scheme is 'http(s)',
    # but we prepend 'azure+' to differentiate https vs azure
    "AZURE": "azure+https://",
    "HTTP": "http://",
    "HTTPS": "https://",
}

TEMP_UNPACK_DIR = Path.home() / ".neuro-tmp"

logger = logging.getLogger(__name__)


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
    asyncio.run(_data_transfer(source, destination))


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
        "DEPRECATED - need for temp dir is automatically detected, "
        "this flag will be removed in a future release. "
        "Download and extract / compress data (if needed) "
        "inside the temporary directory. "
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
    if use_temp_dir:
        logger.warn(
            "Flag -t/--use-temp-dir is DEPRECATED, "
            "and will be REMOVED in a future release. "
            "Need for temp dir is detected automatically from "
            "the source/destination type and compression/extraction flags"
        )
    try:

        async def run_copy() -> None:
            async with get_platform_client() as client:
                operation = CopyOperation(
                    source=source,
                    destination=destination,
                    compress=compress,
                    extract=extract,
                    volumes=list(volume),
                    env=list(env),
                    life_span=life_span,
                    preset=preset,
                    client=client,
                )

                await operation.run()

        asyncio.run(run_copy())
    except Exception as e:
        logger.exception(e)
        raise click.ClickException(f"{e.__class__.__name__}: {e}")


# TODO: (A.K.) implement TransferOperation
async def _data_transfer(src_uri_str: str, dst_uri_str: str) -> None:
    async with get_platform_client() as client:
        src_cluster_or_null = _get_cluster_from_uri(
            client, src_uri_str, scheme="storage"
        )
        dst_cluster = _get_cluster_from_uri(client, dst_uri_str, scheme="storage")

        if not src_cluster_or_null:
            src_cluster = client.cluster_name
        else:
            src_cluster = src_cluster_or_null

    if not dst_cluster:
        raise ValueError(
            f"Invalid destination path {dst_uri_str}: missing cluster name"
        )

    async with get_platform_client(cluster=dst_cluster) as client:
        await client.storage.mkdir(URL("storage:"), parents=True, exist_ok=True)
        await _run_copy_container(src_cluster, src_uri_str, dst_uri_str)


async def _run_copy_container(
    src_cluster: str, src_uri_str: str, dst_uri_str: str
) -> None:
    args = [
        "apolo",
        "run",
        "-s",
        "cpu-small",
        "--pass-config",
        "-v",
        f"{dst_uri_str}:/storage:rw",
        "-e",
        f"APOLO_CLUSTER={src_cluster}",  # inside the job, switch apolo to 'src_cluster'
        "--life-span 10d",
        APOLO_EXTRAS_IMAGE,
        "--",
        f"apolo --show-traceback cp --progress -r -u -T {src_uri_str} /storage",
    ]
    cmd = " ".join(args)
    click.echo(f"Running '{cmd}'")
    subprocess = await asyncio.create_subprocess_shell(cmd)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise click.ClickException("Unable to copy storage")
