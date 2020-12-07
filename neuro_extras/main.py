import logging
from pathlib import Path
from typing import Any, MutableMapping

import toml
from neuromation.cli.click_types import PresetType

from .cli import main


@main.command("init-aliases")
def init_aliases() -> None:
    """
    Create neuro CLI aliases for neuro-extras functionality.
    """
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
            "-f, --file=PATH  path to the Dockerfile within CONTEXT",
            "--build-arg=LIST  build arguments for Docker",
            "-e, --env=LIST  environment variables for container",
            "-v, --volume=LIST  list of volumes for container",
            "-s, --preset=STR  specify  preset for builder container",
            "-F, --force-overwrite  enforce destination image overwrite",
        ],
        "args": "CONTEXT IMAGE_URI",
        "help": (
            "Build docker image on the platform. "
            "Hit `neuro-extras image build --help` for more info."
        ),
    }
    config["alias"]["seldon-init-package"] = {
        "exec": "neuro-extras seldon init-package",
        "args": "URI_OR_PATH",
    }
    config["alias"]["image-transfer"] = {
        "exec": "neuro-extras image transfer",
        "args": "SOURCE DESTINATION",
        "options": [
            "-F, --force-overwrite  enforce destination image overwrite",
        ],
        "help": (
            "Transfer images between the cluster within the platform. "
            "Hit `neuro-extras image transfer --help` for more info."
        ),
    }
    config["alias"]["data-transfer"] = {
        "exec": "neuro-extras data transfer",
        "args": "SOURCE DESTINATION",
    }
    config["alias"]["data-cp"] = {
        "exec": "neuro-extras data cp",
        "options": [
            "-c, --compress Compress source files",
            "-x, --extract Extract downloaded files",
            "-e, --env environment variables for container",
            "-v, --volume list of volumes for container",
            "-t, --use-temp-dir store intermediate data in TMP directory",
        ],
        "args": "SOURCE DESTINATION",
    }
    with toml_path.open("w") as f:
        toml.dump(config, f)
    logger.info(f"Added aliases to {toml_path}")


PRESET = PresetType()

logger = logging.getLogger(__name__)
