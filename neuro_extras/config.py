import json
from typing import Optional

import click
from neuro_cli.asyncio_utils import run as run_async
from neuro_sdk.url_utils import uri_from_cli

from .cli import main
from .image_builder import DockerConfigAuth, ImageBuilder
from .utils import get_neuro_client


@main.group()
def config() -> None:
    """
    Configuration operations.
    """
    pass


@config.command(
    "save-registry-auth",
    help="Save docker auth file for accessing platform registry.",
)
@click.option(
    "--cluster",
    help=(
        "Cluster name for which the auth information should be saved. "
        "Current cluster by default"
    ),
)
@click.argument("path")
def save_registry_auth(path: str, cluster: Optional[str]) -> None:
    run_async(_save_registry_auth(path, cluster))


async def _save_registry_auth(path: str, cluster: Optional[str]) -> None:
    async with get_neuro_client(cluster) as client:
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


@config.command("build-registy-auth")
@click.argument("registry-uri")
@click.argument("username")
@click.argument("password")
def build_registy_auth(registry_uri: str, username: str, password: str) -> None:
    """Generate docker auth for accessing remote registry."""
    auth = _build_registy_auth(registry_uri, username, password)
    click.echo(auth)


def _build_registy_auth(registry_uri: str, username: str, password: str) -> str:
    config = DockerConfigAuth(registry_uri, username, password)
    result = {"auths": {registry_uri: {"auth": config.credentials}}}
    return json.dumps(result)
