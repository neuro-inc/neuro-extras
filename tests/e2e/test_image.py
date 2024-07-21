import logging
import os
import sys
import textwrap
import uuid
from pathlib import Path
from typing import Callable, ContextManager

import pytest
from tenacity import retry, stop_after_attempt, wait_random_exponential

from apolo_extras.const import EX_PLATFORMERROR

from .conftest import CLIRunner, Secret, gen_random_file


LOGGER = logging.getLogger(__name__)


@pytest.mark.serial  # first we build the image, then we are trying to overwrite it
@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@pytest.mark.parametrize("overwrite", [True, False])
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_image_build_overwrite(
    cli_runner: CLIRunner,
    overwrite: bool,
    dockerhub_auth_secret: Secret,
    build_preset: str,
) -> None:
    result = cli_runner(["apolo-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    python_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    img_uri_str = f"image:extras-e2e-overwrite:{sys.platform}-{python_version}-latest"
    build_command = [
        "apolo",
        "image-build",
        "--preset",
        build_preset,
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        ".",
        img_uri_str,
    ]
    if overwrite:
        build_command.insert(2, "-F")

    result = cli_runner(build_command)
    if overwrite:
        assert result.returncode == 0, result
    else:
        assert result.returncode == EX_PLATFORMERROR, result
    try:
        cli_runner(
            ["apolo", "image", "size", img_uri_str],
        )
    finally:
        # Only delete image after second run of the test
        if overwrite is False:
            # (A.K.) on GCP we get Illegal argument(s) ({"errors":
            # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
            # "message":"Manifest is still referenced by tag: v1"}]})
            # cli_runner(["apolo", "image", "rm", img_uri_str])
            pass


@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_ignored_files_are_not_copied(
    cli_runner: CLIRunner,
    dockerhub_auth_secret: Secret,
    build_preset: str,
) -> None:
    result = cli_runner(["apolo-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    ignored_file = Path("this_file_should_not_be_added.txt")
    ignored_file.touch()
    Path(".neuroignore").write_text(f"{ignored_file}\n")

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    Path(dockerfile_path).write_text(
        textwrap.dedent(
            f"""\
            FROM ghcr.io/neuro-inc/alpine:latest
            ADD {random_file_to_disable_layer_caching} /tmp
            ADD . /
            RUN find / -name "*{random_file_to_disable_layer_caching.stem}*"
            RUN find / -name "*{ignored_file.stem}*"
            """
        )
    )

    img_uri_str = f"image:extras-e2e:{uuid.uuid4()}"

    cmd = [
        "apolo",
        "image-build",
        "--preset",
        build_preset,
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        ".",
        img_uri_str,
    ]
    result = cli_runner(
        cmd,
    )
    assert result.returncode == 0, result
    try:
        assert random_file_to_disable_layer_caching.name in result.stdout
        assert ignored_file.name not in result.stdout
    finally:
        # (A.K.) on GCP we get Illegal argument(s) ({"errors":
        # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
        # "message":"Manifest is still referenced by tag: v1"}]})
        # cli_runner(["apolo", "image", "rm", img_uri_str])
        pass


@pytest.mark.serial
@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_image_transfer(
    cli_runner: CLIRunner,
    switch_cluster: Callable[[str], ContextManager[None]],
    current_user: str,
    dockerhub_auth_secret: Secret,
    src_cluster: str,
    dst_cluster: str,
    build_preset: str,
) -> None:
    # Note: we build src image on src_cluster and run transfer job in dst_cluster
    assert src_cluster != dst_cluster

    with switch_cluster(src_cluster):
        result = cli_runner(["apolo-extras", "init-aliases"])
        assert result.returncode == 0, result

        img_name = f"extras-e2e-image-copy:{str(uuid.uuid4())}"
        from_img = f"image:{img_name}"  # also, full src uri is supported
        to_img = f"image://{dst_cluster}/{current_user}/{img_name}"

        dockerfile_path = Path("nested/custom.Dockerfile")
        dockerfile_path.parent.mkdir(parents=True)

        random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
        with open(dockerfile_path, "w") as f:
            f.write(
                textwrap.dedent(
                    f"""\
                    FROM ghcr.io/neuro-inc/alpine:latest
                    ADD {random_file_to_disable_layer_caching} /tmp
                    RUN echo !
                    """
                )
            )

        cmd = [
            "apolo",
            "image-build",
            "--preset",
            build_preset,
            "-e",
            f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
            "-f",
            str(dockerfile_path),
            ".",
            from_img,
        ]
        result = cli_runner(cmd)
        assert result.returncode == 0, result

        try:
            cli_runner(
                ["apolo", "image", "size", from_img],
            )
            result = cli_runner(["apolo", "image-transfer", from_img, to_img])
            assert result.returncode == 0, result
            cli_runner(
                ["apolo", "image", "size", to_img],
            )
        finally:
            # (A.K.) on GCP we get Illegal argument(s) ({"errors":
            # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
            # "message":"Manifest is still referenced by tag: v1"}]})
            # cli_runner(["apolo", "image", "rm", from_img])
            # cli_runner(["apolo", "image", "rm", to_img])
            pass


@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@pytest.mark.parametrize("img_repo_name", ["ne-test-public", "ne-test-private"])
@pytest.mark.parametrize("img_tag", ["", ":latest", ":v1.0.0"])
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_external_image_build(
    cli_runner: CLIRunner,
    dockerhub_auth_secret: Secret,
    img_repo_name: str,
    img_tag: str,
    build_preset: str,
) -> None:
    dckrhb_uname = os.environ["DOCKER_CI_USERNAME"]

    result = cli_runner(["apolo-extras", "init-aliases"])
    assert result.returncode == 0, result

    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    random_file_to_disable_layer_caching = gen_random_file(dockerfile_path.parent)
    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )
    img_uri_str = f"{dckrhb_uname}/{img_repo_name}{img_tag}"
    build_command = [
        "apolo",
        "image-build",
        "--preset",
        build_preset,
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        ".",
        img_uri_str,
    ]
    result = cli_runner(
        build_command,
    )
    if f"Successfully built {img_uri_str}" not in result.stdout:
        LOGGER.warning(result.stdout)
        raise AssertionError("Successfully built message was not found.")


@pytest.mark.skipif(
    sys.platform == "win32", reason="docker is not installed on Windows nodes"
)
@pytest.mark.skipif(
    sys.platform == "darwin", reason="docker is not installed on Mac nodes"
)
@pytest.mark.xfail
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_image_local_build(cli_runner: CLIRunner) -> None:
    dockerfile_path = Path("nested/custom.Dockerfile")
    dockerfile_path.parent.mkdir(parents=True)

    if sys.platform == "win32":
        base_image = "hello-world"
    else:
        base_image = "ghcr.io/neuro-inc/alpine:latest"

    with open(dockerfile_path, "w") as f:
        f.write(
            textwrap.dedent(
                f"""\
                    FROM {base_image}

                    ARG CLOUD_SDK_VERSION=347.0.0
                    ENV CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION

                    RUN echo sdk=$CLOUD_SDK_VERSION
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"
    cmd = [
        "apolo-extras",
        "image",
        "local-build",
        "--verbose",
        "true",
        "-f",
        str(dockerfile_path),
        "--build-arg",
        f"CLOUD_SDK_VERSION=arg-{tag}",
        ".",
        img_uri_str,
    ]
    result = cli_runner(cmd)
    assert result.returncode == 0, result
    try:
        if sys.platform == "win32":
            arg_string = f" --build-arg CLOUD_SDK_VERSION=arg-{tag}"
        else:
            arg_string = f"sdk=arg-{tag}"
        assert arg_string in result.stderr or arg_string in result.stdout
    finally:
        # (A.K.) on GCP we get Illegal argument(s) ({"errors":
        # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
        # "message":"Manifest is still referenced by tag: v1"}]})
        # cli_runner(["apolo", "image", "rm", img_uri_str])
        pass
