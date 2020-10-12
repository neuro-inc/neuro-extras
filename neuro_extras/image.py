import asyncio
import logging
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Sequence

import click
from neuromation import api as neuro_api
from neuromation.api.parsing_utils import _as_repo_str
from neuromation.api.url_utils import uri_from_cli
from neuromation.cli.asyncio_utils import run as run_async
from neuromation.cli.const import EX_PLATFORMERROR

from .cli import main
from .image_builder import ImageBuilder


logger = logging.getLogger(__name__)


@main.group()
def image() -> None:
    pass


@image.command("transfer")
@click.argument("source")
@click.argument("destination")
def image_transfer(source: str, destination: str) -> None:
    run_async(_transfer_image(source, destination))


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


async def _transfer_image(source: str, destination: str) -> None:
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
