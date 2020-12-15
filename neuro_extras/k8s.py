import base64
import json
from pathlib import Path
from typing import Any, Dict

import click
import yaml
from neuro_cli.asyncio_utils import run as run_async

from .cli import main
from .image_builder import ImageBuilder
from .utils import get_neuro_client


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
