import os
import sys
import textwrap
import uuid
from pathlib import Path
from subprocess import CompletedProcess
from typing import Callable, ContextManager

import pytest
from neuro_cli.const import EX_PLATFORMERROR

from .conftest import CLIRunner, Secret, gen_random_file


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_custom_preset(
    cli_runner: CLIRunner,
    repeat_until_success: Callable[..., "CompletedProcess[str]"],
    dockerhub_auth_secret: Secret,
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    # A tricky way to parse neuro config show output and get SECOND preset in a row
    # First one is used by default
    process = cli_runner(["neuro", "config", "show"])
    assert process.returncode == 0, process
    config_lines = [line.strip() for line in process.stdout.splitlines()]
    first_preset_index = config_lines.index("Resource Presets:") + 2
    second_preset_index = first_preset_index + 1
    custom_preset = config_lines[second_preset_index].split()[0]

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    tag = str(uuid.uuid4())

    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    img_name = f"image:extras-e2e-custom-preset-{rnd}"
    img_uri_str = f"{img_name}:{tag}"

    try:
        cmd = [
            "neuro",
            "image-build",
            "--preset",
            custom_preset,
            "-e",
            f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
            "-f",
            str(dockerfile_path),
            ".",
            img_uri_str,
        ]
        result = cli_runner(cmd)
        assert result.returncode == 0, result

        result = repeat_until_success(["neuro", "image", "tags", img_name])
        assert tag in result.stdout
    finally:
        try:
            cli_runner(["neuro", "image", "rm", img_uri_str])
        except Exception:
            # Ignore exception
            pass


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_custom_dockerfile(
    cli_runner: CLIRunner,
    repeat_until_success: Callable[..., "CompletedProcess[str]"],
    dockerhub_auth_secret: Secret,
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    tag = str(uuid.uuid4())

    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    img_name = f"image:extras-e2e-custom-dockerfile-{rnd}"
    img_uri_str = f"{img_name}:{tag}"

    try:
        cmd = [
            "neuro",
            "image-build",
            "-e",
            f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
            "-f",
            str(dockerfile_path),
            ".",
            img_uri_str,
        ]
        result = cli_runner(cmd)
        assert result.returncode == 0, result

        result = repeat_until_success(["neuro", "image", "tags", img_name])
        assert tag in result.stdout
    finally:
        try:
            cli_runner(["neuro", "image", "rm", img_uri_str])
        except Exception:
            # Ignore exception
            pass


@pytest.mark.serial  # first we build the image, then we are trying to overwrite it
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@pytest.mark.parametrize("overwrite", [True, False])
def test_image_build_overwrite(
    cli_runner: CLIRunner,
    repeat_until_success: Callable[..., "CompletedProcess[str]"],
    overwrite: bool,
    dockerhub_auth_secret: Secret,
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    img_name = "image:extras-e2e-overwrite"
    python_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    img_uri_str = f"{img_name}:{sys.platform}-{python_version}-latest"
    build_command = [
        "neuro",
        "image-build",
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        ".",
        img_uri_str,
    ]
    if overwrite:
        build_command.insert(2, "-F")

    try:
        result = cli_runner(build_command)
        if overwrite:
            assert result.returncode == 0, result
        else:
            assert result.returncode == EX_PLATFORMERROR, result

        result = repeat_until_success(["neuro", "image", "tags", img_name])
        assert "latest" in result.stdout
    finally:
        # Only delete image after second run of the test
        if overwrite is False:
            try:
                cli_runner(["neuro", "image", "rm", img_uri_str])
            except Exception:
                # Ignore exception
                pass


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_ignored_files_are_not_copied(
    cli_runner: CLIRunner,
    dockerhub_auth_secret: Secret,
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    ignored_file = "this_file_should_not_be_added.txt"
    ignored_file_content = "this should not be printed\n"

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    Path(".neuroignore").write_text(f"{ignored_file}\n")
    Path(ignored_file).write_text(ignored_file_content)
    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    Path(dockerfile_path).write_text(
        textwrap.dedent(
            f"""\
            FROM ubuntu:latest
            ADD {random_file_to_disable_layer_caching} /tmp
            ADD {ignored_file} /
            RUN cat /{ignored_file}
            """
        )
    )

    img_uri_str = f"image:extras-e2e:{uuid.uuid4()}"

    try:
        cmd = [
            "neuro",
            "image-build",
            "-e",
            f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
            "-f",
            str(dockerfile_path),
            ".",
            img_uri_str,
        ]
        result = cli_runner(cmd)

        assert ignored_file_content not in result.stdout
    finally:
        try:
            cli_runner(["neuro", "image", "rm", img_uri_str])
        except Exception:
            # Ignore exception
            pass


@pytest.mark.serial
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_transfer(
    cli_runner: CLIRunner,
    repeat_until_success: Callable[..., "CompletedProcess[str]"],
    switch_cluster: Callable[[str], ContextManager[None]],
    current_user: str,
    dockerhub_auth_secret: Secret,
) -> None:
    # Note: we build src image on src_cluster and run transfer job in dst_cluster
    src_cluster = os.environ["NEURO_CLUSTER_SECONDARY"]
    dst_cluster = os.environ["NEURO_CLUSTER"]  # can be any other cluster
    assert src_cluster != dst_cluster

    with switch_cluster(src_cluster):
        result = cli_runner(["neuro-extras", "init-aliases"])
        assert result.returncode == 0, result

        # WORKAROUND: Fixing 401 Not Authorized because of this problem:
        # https://github.com/neuromation/platform-registry-api/issues/209
        rnd = uuid.uuid4().hex[:6]
        img_name = f"extras-e2e-image-copy-{rnd}"

        tag = str(uuid.uuid4())
        from_img = f"image:{img_name}:{tag}"  # also, full src uri is supported
        to_img = f"image://{dst_cluster}/{current_user}/{img_name}:{tag}"

        dockerfile_path = Path("nested/custom.Dockerfile")
        dockerfile_path.parent.mkdir(parents=True)

        random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
        with open(dockerfile_path, "w") as f:
            f.write(
                textwrap.dedent(
                    f"""\
                    FROM alpine:latest
                    ADD {random_file_to_disable_layer_caching} /tmp
                    RUN echo !
                    """
                )
            )

        try:
            cmd = [
                "neuro",
                "image-build",
                "-e",
                f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
                "-f",
                str(dockerfile_path),
                ".",
                from_img,
            ]
            result = cli_runner(cmd)
            assert result.returncode == 0, result

            result = repeat_until_success(
                ["neuro", "image", "tags", f"image:{img_name}"]
            )
            assert tag in result.stdout

            # Note: this command switches cluster to destination cluster
            result = cli_runner(["neuro", "image-transfer", from_img, to_img])
            assert result.returncode == 0, result
        finally:
            with switch_cluster(src_cluster):
                try:
                    cli_runner(["neuro", "image", "rm", from_img])
                except Exception:
                    # Ignore exception
                    pass

    try:
        with switch_cluster(dst_cluster):
            result = repeat_until_success(
                ["neuro", "image", "tags", f"image:{img_name}"]
            )
            assert tag in result.stdout
    finally:
        with switch_cluster(dst_cluster):
            try:
                cli_runner(["neuro", "image", "rm", to_img])
            except Exception:
                # Ignore exception
                pass


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_custom_build_args(
    cli_runner: CLIRunner,
    dockerhub_auth_secret: Secret,
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                ARG TEST_ARG
                ARG ANOTHER_TEST_ARG
                RUN echo $TEST_ARG
                RUN echo $ANOTHER_TEST_ARG
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"

    try:
        result = cli_runner(
            [
                "neuro",
                "image-build",
                "-e",
                f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
                "-f",
                str(dockerfile_path),
                "--build-arg",
                f"TEST_ARG=arg-{tag}",
                "--build-arg",
                f"ANOTHER_TEST_ARG=arg-another-{tag}",
                ".",
                img_uri_str,
            ]
        )
        assert result.returncode == 0, result
        assert f"arg-{tag}" in result.stdout
        assert f"arg-another-{tag}" in result.stdout
    finally:
        try:
            cli_runner(["neuro", "image", "rm", img_uri_str])
        except Exception:
            # Ignore exception
            pass


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
def test_image_build_volume(
    cli_runner: CLIRunner,
    temp_random_secret: Secret,
    dockerhub_auth_secret: Secret,
) -> None:
    sec = temp_random_secret

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    result = cli_runner(["neuro", "secret", "add", sec.name, sec.value])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ubuntu:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                ADD secret.txt /
                RUN echo git_token=$(cat secret.txt)
                """
            )
        )

    # WORKAROUND: Fixing 401 Not Authorized because of this problem:
    # https://github.com/neuromation/platform-registry-api/issues/209
    rnd = uuid.uuid4().hex[:6]
    image = f"image:extras-e2e-image-copy-{rnd}"

    tag = str(uuid.uuid4())
    img_uri_str = f"{image}:{tag}"

    try:
        result = cli_runner(
            [
                "neuro",
                "image-build",
                "-e",
                f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
                "-f",
                str(dockerfile_path),
                "-v",
                f"secret:{sec.name}:/kaniko_context/secret.txt",
                ".",
                img_uri_str,
            ]
        )
        assert result.returncode == 0, result
        assert f"git_token={sec.value}" in result.stdout
    finally:
        try:
            cli_runner(["neuro", "image", "rm", img_uri_str])
        except Exception:
            # Ignore exception
            pass


@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@pytest.mark.parametrize("img_repo_name", ["ne-test-public", "ne-test-private"])
@pytest.mark.parametrize("img_tag", ["", ":latest", ":v1.0.0"])
def test_external_image_build(
    cli_runner: CLIRunner,
    dockerhub_auth_secret: Secret,
    img_repo_name: str,
    img_tag: str,
) -> None:
    dckrhb_uname = os.environ["DOCKER_CI_USERNAME"]

    result = cli_runner(["neuro-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )
    img_uri_str = f"{dckrhb_uname}/{img_repo_name}{img_tag}"
    build_command = [
        "neuro",
        "image-build",
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        ".",
        img_uri_str,
    ]
    result = cli_runner(build_command)
    assert f"Successfully built {img_uri_str}" in result.stdout
