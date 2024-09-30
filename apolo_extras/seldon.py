import asyncio
import shutil
from pathlib import Path
from typing import Any, Dict

import click
import yaml
from yarl import URL

from .cli import main
from .common import APOLO_EXTRAS_IMAGE
from .image_builder import ImageBuilder
from .utils import get_platform_client


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
    asyncio.run(_init_seldon_package(path))


@seldon.command("generate-deployment")
@click.option("--name", default="apolo-model")
@click.option("--apolo-secret", default="apolo")
@click.option("--registry-secret", default="apolo-registry")
@click.argument("model-image-uri")
@click.argument("model-storage-uri")
def generate_seldon_deployment(
    name: str,
    apolo_secret: str,
    registry_secret: str,
    model_image_uri: str,
    model_storage_uri: str,
) -> None:
    payload = asyncio.run(
        _create_seldon_deployment(
            name=name,
            apolo_secret=apolo_secret,
            registry_secret_name=registry_secret,
            model_image_uri=model_image_uri,
            model_storage_uri=model_storage_uri,
        )
    )
    click.echo(yaml.dump(payload), nl=False)


async def _init_seldon_package(path: str) -> None:
    async with get_platform_client() as client:
        uri = client.parse.str_to_uri(
            path,
            allowed_schemes=("file", "storage"),
        )
        click.echo(f"Copying a Seldon package scaffolding into {uri}")
        if uri.scheme == "file":
            shutil.copytree(str(SELDON_CUSTOM_PATH), path)
        else:
            await client.storage.mkdir(uri, parents=True)
            await client.storage.upload_dir(URL(SELDON_CUSTOM_PATH.as_uri()), uri)


async def _create_seldon_deployment(
    *,
    name: str,
    apolo_secret: str,
    registry_secret_name: str,
    model_image_uri: str,
    model_storage_uri: str,
) -> Dict[str, Any]:
    async with get_platform_client() as client:
        builder = ImageBuilder.get(local=False)(client)
        model_image_ref = builder.parse_image_ref(model_image_uri)

    pod_spec = {
        "volumes": [
            {"emptyDir": {}, "name": "apolo-storage"},
            {"name": "apolo-secret", "secret": {"secretName": apolo_secret}},
        ],
        "imagePullSecrets": [{"name": registry_secret_name}],
        "initContainers": [
            {
                "name": "apolo-download",
                "image": APOLO_EXTRAS_IMAGE,
                "imagePullPolicy": "Always",
                "securityContext": {"runAsUser": 0},
                "command": ["bash", "-c"],
                "args": [
                    "cp -L -r /var/run/apolo/config /root/.neuro;"
                    "chmod 0700 /root/.neuro;"
                    "chmod 0600 /root/.neuro/db;"
                    f"apolo cp {model_storage_uri} /storage"
                ],
                "volumeMounts": [
                    {"mountPath": "/storage", "name": "apolo-storage"},
                    {"mountPath": "/var/run/apolo/config", "name": "apolo-secret"},
                ],
            }
        ],
        "containers": [
            {
                "name": "model",
                "image": model_image_ref,
                "imagePullPolicy": "Always",
                "volumeMounts": [{"mountPath": "/storage", "name": "apolo-storage"}],
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
