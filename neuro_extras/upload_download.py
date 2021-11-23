import asyncio
from pathlib import Path

import click
from neuro_cli.asyncio_utils import run as run_async
from neuro_sdk import ConfigError, Factory, find_project_root

from .cli import main


@main.command("upload")
@click.argument("path")
def upload(path: str) -> None:
    """
    Upload neuro project files to storage.

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
    Download neuro project files from storage.

    Downloads file (or files under) from storage://remote-project-dir/PATH
    to project-root/PATH. You can use "." for PATH to download whole project.
    The "remote-project-dir" is set using .neuro.toml config, as in example:

    \b
    [extra]
    remote-project-dir = "project-dir-name"
    """
    return_code = run_async(_download(path))
    exit(return_code)


async def _upload(path: str) -> int:
    target = _get_project_root() / path
    if not target.exists():
        raise click.ClickException(f"Folder or file does not exist: {target}")
    remote_project_root = await _get_remote_project_root()
    await _ensure_folder_exists((remote_project_root / path).parent, True)
    if target.is_dir():
        subprocess = await asyncio.create_subprocess_exec(
            "neuro",
            "cp",
            "--recursive",
            "-u",
            str(target),
            "-T",
            f"storage:{remote_project_root / path}",
        )
    else:
        subprocess = await asyncio.create_subprocess_exec(
            "neuro", "cp", str(target), f"storage:{remote_project_root / path}"
        )
    return await subprocess.wait()


async def _download(path: str) -> int:
    project_root = _get_project_root()
    remote_project_root = await _get_remote_project_root()
    await _ensure_folder_exists((project_root / path).parent, False)
    subprocess = await asyncio.create_subprocess_exec(
        "neuro",
        "cp",
        "--recursive",
        "-u",
        f"storage:{remote_project_root / path}",
        "-T",
        str(project_root / path),
    )
    return await subprocess.wait()


def _get_project_root() -> Path:
    try:
        return find_project_root()
    except ConfigError:
        raise click.ClickException(
            "Not a Neu.ro project directory (or any of the parent directories)."
        )


async def _get_remote_project_root() -> Path:
    factory = Factory()
    config = await factory.load_user_config()
    try:
        return Path(config["extra"]["remote-project-dir"])
    except KeyError:
        raise click.ClickException(
            '"remote-project-dir" configuration variable is not set. Please add'
            ' it to "extra" section of project config file.'
        )


async def _ensure_folder_exists(path: Path, remote: bool = False) -> None:
    if remote:
        subprocess = await asyncio.create_subprocess_exec(
            "neuro", "mkdir", "-p", f"storage:{path}"
        )
        returncode = await subprocess.wait()
        if returncode != 0:
            raise click.ClickException("Was unable to create containing directory")
    else:
        path.mkdir(parents=True, exist_ok=True)
