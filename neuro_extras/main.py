from pathlib import Path
from typing import Any, MutableMapping

import click
import toml
from neuromation.cli.asyncio_utils import run as run_async

from neuro_extras.image import _copy_image, image
from neuro_extras.log import logger
from neuro_extras.storage import _copy_storage

from .cli import main


@main.command("cp")
@click.argument("source")
@click.argument("destination")
def cluster_copy(source: str, destination: str) -> None:
    run_async(_copy_storage(source, destination))


@image.command("copy")
@click.argument("source")
@click.argument("destination")
def image_copy(source: str, destination: str) -> None:
    run_async(_copy_image(source, destination))


@main.command("init-aliases")
def init_aliases() -> None:
    # TODO: support patching the global ~/.neuro/user.toml
    toml_path = Path.cwd() / ".neuro.toml"
    config: MutableMapping[str, Any] = {}
    if toml_path.exists():
        with toml_path.open("r") as f:
            config = toml.load(f)
    config.setdefault("alias", {})
    config["alias"]["image-build"] = {
        "exec": "neuro-extras image build",
        "options": [
            "-f, --file path to the Dockerfile within CONTEXT",
            "--build-arg build arguments for Docker",
            "-e, --env environment variables for container",
            "-v, --volume list of volumes for container",
        ],
        "args": "CONTEXT IMAGE_URI",
    }
    config["alias"]["seldon-init-package"] = {
        "exec": "neuro-extras seldon init-package",
        "args": "URI_OR_PATH",
    }
    config["alias"]["image-copy"] = {
        "exec": "neuro-extras image copy",
        "args": "SOURCE DESTINATION",
    }
    config["alias"]["storage-cp"] = {
        "exec": "neuro-extras cp",
        "args": "SOURCE DESTINATION",
    }
    with toml_path.open("w") as f:
        toml.dump(config, f)
    logger.info(f"Added aliases to {toml_path}")
