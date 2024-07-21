import asyncio
import json
from typing import Optional

import click

from .cli import main
from .image_builder import DockerConfigAuth, ImageBuilder
from .utils import get_platform_client


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
    asyncio.run(_save_registry_auth(path, cluster))


async def _save_registry_auth(path: str, cluster: Optional[str]) -> None:
    async with get_platform_client(cluster) as client:
        uri = client.parse.str_to_uri(
            path,
            allowed_schemes=("file", "storage"),
        )
        builder = ImageBuilder.get(local=False)(client)
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
