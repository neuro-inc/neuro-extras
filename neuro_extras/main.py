import logging
from pathlib import Path
from typing import Any, MutableMapping, Sequence

import click
import toml
import yaml
from neuromation.cli.asyncio_utils import run as run_async

from neuro_extras.config import _save_docker_json
from neuro_extras.data import _data_cp
from neuro_extras.image import _build_image, _copy_image
from neuro_extras.k8s import _create_k8s_registry_secret, _create_k8s_secret
from neuro_extras.seldon import _create_seldon_deployment, _init_seldon_package
from neuro_extras.storage import _copy_storage
from neuro_extras.upload_download import _download, _upload


logger = logging.getLogger(__name__)

NEURO_EXTRAS_IMAGE_TAG = "v20.9.30a7"
NEURO_EXTRAS_IMAGE = f"neuromation/neuro-extras:{NEURO_EXTRAS_IMAGE_TAG}"


class ClickLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            click.echo(msg)
        except Exception:
            self.handleError(record)


@click.group()
def main() -> None:
    handler = ClickLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


@main.command("cp")
@click.argument("source")
@click.argument("destination")
def cluster_copy(source: str, destination: str) -> None:
    run_async(_copy_storage(source, destination))


@main.group()
def seldon() -> None:
    pass


@main.group()
def data() -> None:
    pass


@main.group()
def image() -> None:
    pass


@main.group()
def config() -> None:
    pass


@main.group()
def k8s() -> None:
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


@main.command("upload")
@click.argument("path")
def upload(path: str) -> None:
    """
    Upload neuro project files to storage

    Uploads file (or files under) project-root/PATH to
    storage://remote-project-dir/PATH. You can use "." for PATH to upload
    whole project. The "remote-project-dir" is set using .neuro.toml config,
    as in example:

    \b
    [extra]
    remote-project-dir = "project-dir-name"
    """
    return_code = run_async(_upload(path))
    exit(return_code)


@main.command("download")
@click.argument("path")
def download(path: str) -> None:
    """
    Download neuro project files from storage

    Downloads file (or files under) from storage://remote-project-dir/PATH
    to project-root/PATH. You can use "." for PATH to download whole project.
    The "remote-project-dir" is set using .neuro.toml config, as in example:

    \b
    [extra]
    remote-project-dir = "project-dir-name"
    """
    return_code = run_async(_download(path))
    exit(return_code)


@config.command("save-docker-json")
@click.argument("path")
def config_save_docker_json(path: str) -> None:
    run_async(_save_docker_json(path))


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


@data.command("cp")
@click.argument("source")
@click.argument("destination")
@click.option("-x", "--extract", default=False, is_flag=True)
@click.option("-c", "--compress", default=False, is_flag=True)
@click.option(
    "-v",
    "--volume",
    metavar="MOUNT",
    multiple=True,
    help=(
        "Mounts directory from vault into container. "
        "Use multiple options to mount more than one volume. "
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
def data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: Sequence[str],
    env: Sequence[str],
) -> None:
    if extract and compress:
        raise click.ClickException("Extract and compress can't be used together")
    run_async(_data_cp(source, destination, extract, compress, volume, env))


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
