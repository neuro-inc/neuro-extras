import json

import click
from neuro_cli.asyncio_utils import run as run_async
from neuro_sdk.url_utils import uri_from_cli

from .cli import main
from .image_builder import ImageBuilder
from .utils import get_neuro_client


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
