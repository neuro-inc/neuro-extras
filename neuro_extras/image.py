import logging
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Optional, Sequence, Tuple

import click
import neuro_sdk as neuro_api
from neuro_cli.asyncio_utils import run as run_async
from neuro_cli.const import EX_OK, EX_PLATFORMERROR
from neuro_sdk.client import Client
from neuro_sdk.url_utils import uri_from_cli

from .cli import main
from .image_builder import DockerConfigAuth, ImageBuilder, create_docker_config_auth
from .utils import get_neuro_client


logger = logging.getLogger(__name__)


@main.group()
def image() -> None:
    """
    Job container image operations.
    """
    pass


@image.command("transfer")
@click.argument("source")
@click.argument("destination")
@click.option(
    "-F",
    "--force-overwrite",
    default=False,
    is_flag=True,
    help="Transfer even if the destination image already exists.",
)
def image_transfer(source: str, destination: str, force_overwrite: bool) -> None:
    """
    Copy images between clusters.
    """
    exit_code = run_async(_image_transfer(source, destination, force_overwrite))
    sys.exit(exit_code)


@image.command(
    "build", help="Build Job container image remotely on cluster using Kaniko."
)
@click.argument("path", metavar="CONTEXT_PATH")
@click.argument("image_uri")
@click.option(
    "-f",
    "--file",
    default="Dockerfile",
    show_default=True,
    help=(
        "Relative (w.r.t. context) path to the dockerfile. "
        "The dockerfile should be within the context directory."
    ),
)
@click.option(
    "--build-arg",
    multiple=True,
    metavar="VAR=VAL",
    help=(
        "Buid-time variables passed in ARG values, similarly to Docker. "
        "Could be used multiple times for multiple arguments."
    ),
)
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
@click.option(
    "-s",
    "--preset",
    metavar="PRESET",
    help=(
        "Predefined resource configuration (to see available values, "
        "run `neuro config show`)"
    ),
)
@click.option(
    "-F",
    "--force-overwrite",
    default=False,
    show_default=True,
    is_flag=True,
    help="Overwrite if the destination image already exists.",
)
@click.option(
    "--cache/--no-cache",
    default=True,
    show_default=True,
    help="Use Kaniko cache while building image.",
)
@click.option(
    "--verbose",
    type=bool,
    default=False,
    help="If specified, run Kaniko with 'debug' verbosity, otherwise 'info' (default).",
)
@click.option(
    "--build-tag",
    multiple=True,
    metavar="VAR=VAL",
    help=(
        "Set tag(s) for image builder job. "
        "We will add tag 'kaniko-builds:{image-name}' authomatically."
    ),
)
def image_build(
    path: str,
    image_uri: str,
    file: str,
    build_arg: Tuple[str],
    volume: Tuple[str],
    env: Tuple[str],
    preset: str,
    force_overwrite: bool,
    cache: bool,
    verbose: bool,
    build_tag: Tuple[str],
) -> None:
    try:
        sys.exit(
            run_async(
                _build_image(
                    dockerfile_path=Path(file),
                    context=path,
                    image_uri_str=image_uri,
                    use_cache=cache,
                    build_args=build_arg,
                    volume=volume,
                    env=env,
                    preset=preset,
                    force_overwrite=force_overwrite,
                    verbose=verbose,
                    build_tags=build_tag,
                )
            )
        )
    except (ValueError, click.ClickException) as e:
        logger.error(f"Failed to build image: {e}")
        sys.exit(EX_PLATFORMERROR)


async def _parse_neuro_image(image: str) -> neuro_api.RemoteImage:
    async with get_neuro_client() as client:
        return client.parse.remote_image(image)


def _get_cluster_from_uri(image_uri: str, *, scheme: str) -> Optional[str]:
    try:
        uri = uri_from_cli(image_uri, "", "", allowed_schemes=[scheme])
        return uri.host
    except ValueError:
        # seems like the image scheme was not provided, since it's hosted in dockerhub
        logger.warning(f"Unable to parse the cluster name from URI '{image_uri}' ")
        return None


async def _image_transfer(
    src_uri_str: str, dst_uri_str: str, force_overwrite: bool
) -> int:
    src_cluster: Optional[str] = _get_cluster_from_uri(src_uri_str, scheme="image")
    dst_cluster: Optional[str] = _get_cluster_from_uri(dst_uri_str, scheme="image")
    if not dst_cluster:
        raise ValueError(
            f"Invalid destination image {dst_uri_str}: missing cluster name"
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        async with get_neuro_client(cluster=src_cluster) as src_client:
            src_image = src_client.parse.remote_image(image=src_uri_str)
            src_reg_auth = await create_docker_config_auth(src_client.config)

        dockerfile = Path(f"{tmpdir}/Dockerfile")
        dockerfile.write_text(
            textwrap.dedent(
                f"""\
                FROM {src_image.as_docker_url()}
                LABEL neu.ro/source-image-uri={src_uri_str}
                """
            )
        )
        migration_job_tags = (
            f"src-image:{src_image}",
            f"neuro-extras:image-transfer",
        )
        return await _build_image(
            dockerfile_path=Path(dockerfile.name),
            context=tmpdir,
            image_uri_str=dst_uri_str,
            use_cache=True,
            build_args=(),
            volume=(),
            env=(),
            build_tags=migration_job_tags,
            force_overwrite=force_overwrite,
            registry_auths=[src_reg_auth],
        )


async def _build_image(
    dockerfile_path: Path,
    context: str,
    image_uri_str: str,
    use_cache: bool,
    build_args: Tuple[str, ...],
    volume: Tuple[str, ...],
    env: Tuple[str, ...],
    build_tags: Tuple[str, ...],
    force_overwrite: bool,
    preset: Optional[str] = None,
    registry_auths: Sequence[DockerConfigAuth] = (),
    verbose: bool = False,
) -> int:
    cluster = _get_cluster_from_uri(image_uri_str, scheme="image")
    async with get_neuro_client(cluster=cluster) as client:
        context_uri = uri_from_cli(
            context,
            client.username,
            client.cluster_name,
            allowed_schemes=("file", "storage"),
        )
        image_exists = await _check_image_exists(image_uri_str, client)
        if image_exists and force_overwrite:
            logger.warning(
                f"Target image '{image_uri_str}' exists and will be overwritten."
            )
        elif image_exists and not force_overwrite:
            raise click.ClickException(
                f"Target image '{image_uri_str}' exists. "
                f"Use -F/--force-overwrite flag to enforce overwriting."
            )

        builder = ImageBuilder(
            client, extra_registry_auths=registry_auths, verbose=verbose
        )
        exit_code = await builder.build(
            dockerfile_path=dockerfile_path,
            context_uri=context_uri,
            image_uri_str=image_uri_str,
            use_cache=use_cache,
            build_args=build_args,
            volumes=volume,
            envs=env,
            job_preset=preset,
            build_tags=build_tags,
        )
        if exit_code == EX_OK:
            logger.info(f"Successfully built {image_uri_str}")
            return EX_OK
        else:
            raise click.ClickException(f"Failed to build image: {exit_code}")


async def _check_image_exists(image_uri_str: str, client: Client) -> bool:
    image = await _parse_neuro_image(image_uri_str)
    if image.registry is None:
        # TODO (y.s.): we might need to implement this check later.
        logger.warning(
            f"Skipping check if image '{image}' exists. "
            "If it does exist - it will be overwritten!"
        )
        return False
    try:
        existing_images = await client.images.tags(
            neuro_api.RemoteImage(
                name=image.name,
                owner=image.owner,
                cluster_name=image.cluster_name,
                registry=image.registry,
                tag=None,
            )
        )
        return image in existing_images
    except neuro_api.errors.ResourceNotFound:
        # image does not exists on platform registry
        return False
