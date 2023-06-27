import logging
import os
import sys
import textwrap
import uuid
from pathlib import Path
from typing import Callable, ContextManager, Optional

import pytest
from tenacity import retry, stop_after_attempt, wait_random_exponential

from neuro_extras.const import EX_PLATFORMERROR

from .conftest import CLIRunner, Secret, gen_random_file


LOGGER = logging.getLogger(__name__)

NEURO_EXTRAS_PRESET: Optional[str] = os.environ.get("NEURO_EXTRAS_PRESET")

PRESET_ARG = ["--preset", NEURO_EXTRAS_PRESET] if NEURO_EXTRAS_PRESET else []


@pytest.mark.smoke
@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_image_build_custom_preset(
    cli_runner: CLIRunner,
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
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"

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

    try:
        cli_runner(
            ["neuro", "image", "size", img_uri_str],
        )
    finally:
        # (A.K.) on GCP we get Illegal argument(s) ({"errors":
        # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
        # "message":"Manifest is still referenced by tag: v1"}]})
        # cli_runner(["neuro", "image", "rm", img_uri_str])
        pass


@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_image_build_custom_dockerfile(
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
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    tag = str(uuid.uuid4())

    img_uri_str = f"image:extras-e2e:{tag}"

    cmd = [
        "neuro",
        "image-build",
        *PRESET_ARG,
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        ".",
        img_uri_str,
    ]
    result = cli_runner(cmd)
    assert result.returncode == 0, result
    try:
        cli_runner(
            ["neuro", "image", "size", img_uri_str],
        )
    finally:
        # (A.K.) on GCP we get Illegal argument(s) ({"errors":
        # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
        # "message":"Manifest is still referenced by tag: v1"}]})
        # cli_runner(["neuro", "image", "rm", img_uri_str])
        pass


@pytest.mark.serial  # first we build the image, then we are trying to overwrite it
@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@pytest.mark.parametrize("overwrite", [True, False])
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_image_build_overwrite(
    cli_runner: CLIRunner,
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
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )

    python_version = f"{sys.version_info[0]}.{sys.version_info[1]}"
    img_uri_str = f"image:extras-e2e-overwrite:{sys.platform}-{python_version}-latest"
    build_command = [
        "neuro",
        "image-build",
        *PRESET_ARG,
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
            ["neuro", "image", "size", img_uri_str],
        )
    finally:
        # Only delete image after second run of the test
        if overwrite is False:
            # (A.K.) on GCP we get Illegal argument(s) ({"errors":
            # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
            # "message":"Manifest is still referenced by tag: v1"}]})
            # cli_runner(["neuro", "image", "rm", img_uri_str])
            pass


@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
def test_ignored_files_are_not_copied(
    cli_runner: CLIRunner,
    dockerhub_auth_secret: Secret,
) -> None:
    result = cli_runner(["neuro-extras", "init-aliases"])
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
        "neuro",
        "image-build",
        *PRESET_ARG,
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
        # cli_runner(["neuro", "image", "rm", img_uri_str])
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
) -> None:
    # Note: we build src image on src_cluster and run transfer job in dst_cluster
    assert src_cluster != dst_cluster

    with switch_cluster(src_cluster):
        result = cli_runner(["neuro-extras", "init-aliases"])
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
            "neuro",
            "image-build",
            *PRESET_ARG,
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
                ["neuro", "image", "size", from_img],
            )
            result = cli_runner(["neuro", "image-transfer", from_img, to_img])
            assert result.returncode == 0, result
            cli_runner(
                ["neuro", "image", "size", to_img],
            )
        finally:
            # (A.K.) on GCP we get Illegal argument(s) ({"errors":
            # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
            # "message":"Manifest is still referenced by tag: v1"}]})
            # cli_runner(["neuro", "image", "rm", from_img])
            # cli_runner(["neuro", "image", "rm", to_img])
            pass


@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
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
                FROM ghcr.io/neuro-inc/alpine:latest
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
    cmd = [
        "neuro",
        "image-build",
        *PRESET_ARG,
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
    result = cli_runner(cmd)
    assert result.returncode == 0, result
    try:
        assert f"arg-{tag}" in result.stdout
        assert f"arg-another-{tag}" in result.stdout
    finally:
        # (A.K.) on GCP we get Illegal argument(s) ({"errors":
        # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
        # "message":"Manifest is still referenced by tag: v1"}]})
        # cli_runner(["neuro", "image", "rm", img_uri_str])
        pass


@pytest.mark.xfail
@pytest.mark.skipif(sys.platform == "win32", reason="kaniko does not work on Windows")
@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=10, max=60))
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
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                ADD secret.txt /
                RUN echo git_token=$(cat secret.txt)
                """
            )
        )

    tag = str(uuid.uuid4())
    img_uri_str = f"image:extras-e2e:{tag}"
    cmd = [
        "neuro",
        "image-build",
        *PRESET_ARG,
        "-e",
        f"{dockerhub_auth_secret.name}=secret:{dockerhub_auth_secret.name}",
        "-f",
        str(dockerfile_path),
        "-v",
        f"secret:{sec.name}:/kaniko_context/secret.txt",
        ".",
        img_uri_str,
    ]
    result = cli_runner(cmd)
    assert result.returncode == 0, result
    try:
        assert f"git_token={sec.value}" in result.stdout
    finally:
        # (A.K.) on GCP we get Illegal argument(s) ({"errors":
        # [{"code":"GOOGLE_MANIFEST_DANGLING_TAG",
        # "message":"Manifest is still referenced by tag: v1"}]})
        # cli_runner(["neuro", "image", "rm", img_uri_str])
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
                FROM ghcr.io/neuro-inc/alpine:latest
                ADD {random_file_to_disable_layer_caching} /tmp
                RUN echo !
                """
            )
        )
    img_uri_str = f"{dckrhb_uname}/{img_repo_name}{img_tag}"
    build_command = [
        "neuro",
        "image-build",
        *PRESET_ARG,
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
        "neuro-extras",
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
        # cli_runner(["neuro", "image", "rm", img_uri_str])
        pass
