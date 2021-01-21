from distutils import dir_util
from pathlib import Path
from typing import Any, Dict

import click
import yaml
from neuro_cli.asyncio_utils import run as run_async
from neuro_sdk.url_utils import uri_from_cli
from yarl import URL

from .cli import main
from .common import NEURO_EXTRAS_IMAGE
from .image_builder import ImageBuilder
from .utils import get_neuro_client


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
                "securityContext": {"runAsUser": 0},
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
