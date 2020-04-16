import logging
import os
import uuid
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
from typing import Callable, Iterator, List

import pytest
from _pytest.capture import CaptureFixture
from neuromation.cli.const import EX_OK
from neuromation.cli.main import cli as neuro_main

from neuro_extras.main import main as extras_main


logger = logging.getLogger(__name__)


@pytest.fixture
def project_dir() -> Iterator[Path]:
    with TemporaryDirectory() as cwd_str:
        old_cwd = Path.cwd()
        cwd = Path(cwd_str)
        os.chdir(cwd)
        try:
            yield cwd
        finally:
            os.chdir(old_cwd)


CLIRunner = Callable[[List[str]], CompletedProcess]


@pytest.fixture()
def cli_runner(capfd: CaptureFixture, project_dir: Path) -> CLIRunner:
    def _run_cli(args: List[str]) -> CompletedProcess:  # type: ignore
        cmd = args.pop(0)
        if cmd not in ("neuro", "neuro-extras"):
            pytest.fail(f"Illegal command: {cmd}")

        logger.info(f"Run '{cmd} {' '.join(args)}'",)
        capfd.readouterr()

        main = extras_main
        if cmd == "neuro":
            args = [
                "--show-traceback",
                "--disable-pypi-version-check",
                "--color=no",
            ] + args
            main = neuro_main

        code = EX_OK
        try:
            main(args)
        except SystemExit as e:
            code = e.code
        out, err = capfd.readouterr()
        return CompletedProcess(
            args=[cmd] + args, returncode=code, stdout=out.strip(), stderr=err.strip()
        )

    return _run_cli


def test_init_aliases(cli_runner: CLIRunner) -> None:
    toml_path = Path(".neuro.toml")
    assert not toml_path.exists()

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result
    assert "Added aliases to" in result.stdout, result

    assert toml_path.exists()


def test_image_build_failure(cli_runner: CLIRunner) -> None:
    pkg_path = Path("pkg")
    result = cli_runner(["neuro-extras", "seldon", "init-package", str(pkg_path)])
    result = cli_runner(["neuro-extras", "image", "build", str(pkg_path), "<invalid>"])
    assert result.returncode == 1, result
    assert "repository can only contain" in result.stdout


def test_seldon_deploy_from_local(cli_runner: CLIRunner) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    pkg_path = Path("pkg")
    img_uri_str = f"image:extras-e2e:{uuid.uuid4()}"
    result = cli_runner(["neuro", "seldon-init-package", str(pkg_path)])
    assert result.returncode == 0, result
    assert "Copying a Seldon package scaffolding" in result.stdout, result

    assert (pkg_path / "Dockerfile").exists()

    result = cli_runner(["neuro", "image-build", str(pkg_path), img_uri_str])
    assert result.returncode == 0, result
