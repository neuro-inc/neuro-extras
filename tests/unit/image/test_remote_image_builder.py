from pathlib import Path
from unittest import mock

import pytest
from yarl import URL

from apolo_extras.image import _build_image
from apolo_extras.image_builder import ImageBuilder


async def test_image_builder__min_parameters(
    remote_image_builder: ImageBuilder,
) -> None:
    context = "/path/to/context"

    await _build_image(
        dockerfile_path=Path("path/to/Dockerfile"),
        context=context,
        image_uri_str="image:targetimage:latest",
        use_cache=True,
        build_args=(),
        volume=(),
        env=(),
        build_tags=(),
        force_overwrite=False,
    )

    expected_storage_build_root = URL(
        "storage://mycluster/myproject/.builds/mocked-uuid-4"
    )
    storage_mkdir_mock: mock.AsyncMock = remote_image_builder._client.storage.mkdir  # type: ignore # noqa: E501
    storage_mkdir_mock.assert_awaited_once_with(
        expected_storage_build_root, parents=True
    )
    storage_create_mock: mock.AsyncMock = remote_image_builder._client.storage.create  # type: ignore # noqa: E501
    storage_create_mock.assert_awaited_once_with(
        expected_storage_build_root / ".docker.config.json",
        mock.ANY,
    )
    subproc_mock: mock.AsyncMock = remote_image_builder._execute_subprocess  # type: ignore # noqa: E501
    assert subproc_mock.await_count == 2
    upload_ctx_cmd = subproc_mock.await_args_list[0][0][0]
    assert upload_ctx_cmd == [
        "apolo",
        "--disable-pypi-version-check",
        "cp",
        "--recursive",
        Path(context).resolve().as_uri(),
        str(expected_storage_build_root / "context"),
    ]
    start_build_cmd = subproc_mock.await_args_list[1][0][0]
    start_build_apolo_args = start_build_cmd[: start_build_cmd.index("--")]
    start_build_job_arg = start_build_cmd[start_build_cmd.index("--") + 1 :][0]
    start_build_kaniko_args = start_build_job_arg.split(" ")
    assert start_build_apolo_args == [
        "apolo",
        "--disable-pypi-version-check",
        "job",
        "run",
        "--life-span=4h",
        "--schedule-timeout=20m",
        "--project=myproject",
        "--tag=kaniko-builds-image:image://mycluster/myproject/targetimage:latest",
        "--volume=storage://mycluster/myproject/.builds/mocked-uuid-4/.docker.config.json:/kaniko/.docker/config.json:ro",  # noqa: E501
        "--volume=storage://mycluster/myproject/.builds/mocked-uuid-4/context:/kaniko_context:rw",  # noqa: E501
        "--env=container=docker",
        "gcr.io/kaniko-project/executor:v1.20.0-debug",
    ]
    assert start_build_kaniko_args == [
        "--context=/kaniko_context",
        "--dockerfile=/kaniko_context/path/to/Dockerfile",
        "--destination=registry.mycluster.noexists/myproject/targetimage:latest",
        "--cache=true",
        "--cache-repo=registry.mycluster.noexists/myproject/layer-cache/cache",
        "--verbosity=info",
        "--image-fs-extract-retry=1",
        "--push-retry=3",
        "--use-new-run=true",
        "--snapshot-mode=redo",
    ]


async def test_image_builder__full_parameters(
    remote_image_builder: ImageBuilder,
) -> None:
    context = "/path/to/context"

    await _build_image(
        dockerfile_path=Path("path/to/Dockerfile"),
        context=context,
        image_uri_str="image:targetimage:latest",
        use_cache=True,
        build_args=("ARG1=ARGVAL1", "ARG2=ARGVAL2"),
        volume=(
            "storage:somevol:/mnt/vol1",
            "storage:/someproject2/somevol2:/mnt/vol2",
        ),
        env=("ENV1=VAL1", "ENV2=VAL2"),
        preset="custom-preset",
        build_tags=("tag1", "tag2"),
        project_name="myproject",
        extra_kaniko_args="--some-kaniko-arg1 --some-kaniko-arg2=arg2val",
        force_overwrite=False,
    )

    expected_storage_build_root = URL(
        "storage://mycluster/myproject/.builds/mocked-uuid-4"
    )
    storage_mkdir_mock: mock.AsyncMock = remote_image_builder._client.storage.mkdir  # type: ignore # noqa: E501
    storage_mkdir_mock.assert_awaited_once_with(
        expected_storage_build_root, parents=True
    )
    storage_create_mock: mock.AsyncMock = remote_image_builder._client.storage.create  # type: ignore # noqa: E501
    storage_create_mock.assert_awaited_once_with(
        expected_storage_build_root / ".docker.config.json",
        mock.ANY,
    )
    subproc_mock: mock.AsyncMock = remote_image_builder._execute_subprocess  # type: ignore # noqa: E501
    assert subproc_mock.await_count == 2
    upload_ctx_cmd = subproc_mock.await_args_list[0][0][0]
    assert upload_ctx_cmd == [
        "apolo",
        "--disable-pypi-version-check",
        "cp",
        "--recursive",
        Path(context).resolve().as_uri(),
        str(expected_storage_build_root / "context"),
    ]
    start_build_cmd = subproc_mock.await_args_list[1][0][0]
    start_build_apolo_args = start_build_cmd[: start_build_cmd.index("--")]
    start_build_job_arg = start_build_cmd[start_build_cmd.index("--") + 1 :][0]
    start_build_kaniko_args = start_build_job_arg.split(" ")
    assert start_build_apolo_args == [
        "apolo",
        "--disable-pypi-version-check",
        "job",
        "run",
        "--life-span=4h",
        "--schedule-timeout=20m",
        "--project=myproject",
        "--preset=custom-preset",
        "--tag=tag1",
        "--tag=tag2",
        "--tag=kaniko-builds-image:image://mycluster/myproject/targetimage:latest",
        "--volume=storage:somevol:/mnt/vol1",
        "--volume=storage:/someproject2/somevol2:/mnt/vol2",
        "--volume=storage://mycluster/myproject/.builds/mocked-uuid-4/.docker.config.json:/kaniko/.docker/config.json:ro",  # noqa: E501
        "--volume=storage://mycluster/myproject/.builds/mocked-uuid-4/context:/kaniko_context:rw",  # noqa: E501
        "--env=ENV1=VAL1",
        "--env=ENV2=VAL2",
        "--env=container=docker",
        "gcr.io/kaniko-project/executor:v1.20.0-debug",
    ]
    assert start_build_kaniko_args == [
        "--context=/kaniko_context",
        "--dockerfile=/kaniko_context/path/to/Dockerfile",
        "--destination=registry.mycluster.noexists/myproject/targetimage:latest",
        "--cache=true",
        "--cache-repo=registry.mycluster.noexists/myproject/layer-cache/cache",
        "--verbosity=info",
        "--image-fs-extract-retry=1",
        "--push-retry=3",
        "--use-new-run=true",
        "--snapshot-mode=redo",
        "--build-arg",
        "ARG1=ARGVAL1",
        "--build-arg",
        "ARG2=ARGVAL2",
        "--build-arg",
        "ENV1",
        "--build-arg",
        "ENV2",
        "--some-kaniko-arg1",
        "--some-kaniko-arg2=arg2val",
    ]


async def test_image_builder__conflicting_kaniko_args(
    remote_image_builder: ImageBuilder,
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "Extra kaniko arguments {'--image-fs-extract-retry'} overlap "
            "with autogenerated arguments. Please remove them "
            "in order to proceed or contact the support team."
        ),
    ):
        await _build_image(
            dockerfile_path=Path("path/to/Dockerfile"),
            context="/path/to/context",
            image_uri_str="image:targetimage:latest",
            use_cache=True,
            build_args=(),
            volume=(),
            env=(),
            preset=None,
            build_tags=(),
            project_name="myproject",
            extra_kaniko_args="--image-fs-extract-retry=3",
            force_overwrite=False,
        )


async def test_image_builder__custom_project(
    remote_image_builder: ImageBuilder,
) -> None:
    context = "/path/to/context"
    await _build_image(
        dockerfile_path=Path("path/to/Dockerfile"),
        context=context,
        image_uri_str="image:targetimage:latest",
        use_cache=True,
        build_args=(),
        volume=(),
        env=(),
        build_tags=(),
        force_overwrite=False,
        project_name="otherproject",
    )

    expected_storage_build_root = URL(
        "storage://mycluster/otherproject/.builds/mocked-uuid-4"
    )
    storage_mkdir_mock: mock.AsyncMock = remote_image_builder._client.storage.mkdir  # type: ignore # noqa: E501
    storage_mkdir_mock.assert_awaited_once_with(
        expected_storage_build_root, parents=True
    )
    storage_create_mock: mock.AsyncMock = remote_image_builder._client.storage.create  # type: ignore # noqa: E501
    storage_create_mock.assert_awaited_once_with(
        expected_storage_build_root / ".docker.config.json",
        mock.ANY,
    )
    subproc_mock: mock.AsyncMock = remote_image_builder._execute_subprocess  # type: ignore # noqa: E501
    assert subproc_mock.await_count == 2
    upload_ctx_cmd = subproc_mock.await_args_list[0][0][0]
    assert upload_ctx_cmd == [
        "apolo",
        "--disable-pypi-version-check",
        "cp",
        "--recursive",
        Path(context).resolve().as_uri(),
        str(expected_storage_build_root / "context"),
    ]
    start_build_cmd = subproc_mock.await_args_list[1][0][0]
    start_build_apolo_args = start_build_cmd[: start_build_cmd.index("--")]
    start_build_job_arg = start_build_cmd[start_build_cmd.index("--") + 1 :][0]
    start_build_kaniko_args = start_build_job_arg.split(" ")
    assert start_build_apolo_args == [
        "apolo",
        "--disable-pypi-version-check",
        "job",
        "run",
        "--life-span=4h",
        "--schedule-timeout=20m",
        "--project=otherproject",
        "--tag=kaniko-builds-image:image://mycluster/otherproject/targetimage:latest",
        "--volume=storage://mycluster/otherproject/.builds/mocked-uuid-4/.docker.config.json:/kaniko/.docker/config.json:ro",  # noqa: E501
        "--volume=storage://mycluster/otherproject/.builds/mocked-uuid-4/context:/kaniko_context:rw",  # noqa: E501
        "--env=container=docker",
        "gcr.io/kaniko-project/executor:v1.20.0-debug",
    ]
    assert start_build_kaniko_args == [
        "--context=/kaniko_context",
        "--dockerfile=/kaniko_context/path/to/Dockerfile",
        "--destination=registry.mycluster.noexists/otherproject/targetimage:latest",
        "--cache=true",
        "--cache-repo=registry.mycluster.noexists/otherproject/layer-cache/cache",
        "--verbosity=info",
        "--image-fs-extract-retry=1",
        "--push-retry=3",
        "--use-new-run=true",
        "--snapshot-mode=redo",
    ]


async def test_image_builder__storage_context(
    remote_image_builder: ImageBuilder,
) -> None:
    context_uri_str = "storage:context"
    await _build_image(
        dockerfile_path=Path("path/to/Dockerfile"),
        context=context_uri_str,
        image_uri_str="image:targetimage:latest",
        use_cache=True,
        build_args=(),
        volume=(),
        env=(),
        build_tags=(),
        force_overwrite=False,
    )

    expected_storage_build_root = URL(
        "storage://mycluster/myproject/.builds/mocked-uuid-4"
    )
    storage_mkdir_mock: mock.AsyncMock = remote_image_builder._client.storage.mkdir  # type: ignore # noqa: E501
    storage_mkdir_mock.assert_awaited_once_with(
        expected_storage_build_root, parents=True
    )
    storage_create_mock: mock.AsyncMock = remote_image_builder._client.storage.create  # type: ignore # noqa: E501
    storage_create_mock.assert_awaited_once_with(
        expected_storage_build_root / ".docker.config.json",
        mock.ANY,
    )
    subproc_mock: mock.AsyncMock = remote_image_builder._execute_subprocess  # type: ignore # noqa: E501
    assert subproc_mock.await_count == 1
    start_build_cmd = subproc_mock.await_args_list[0][0][0]
    start_build_apolo_args = start_build_cmd[: start_build_cmd.index("--")]
    start_build_job_arg = start_build_cmd[start_build_cmd.index("--") + 1 :][0]
    start_build_kaniko_args = start_build_job_arg.split(" ")
    assert start_build_apolo_args == [
        "apolo",
        "--disable-pypi-version-check",
        "job",
        "run",
        "--life-span=4h",
        "--schedule-timeout=20m",
        "--project=myproject",
        "--tag=kaniko-builds-image:image://mycluster/myproject/targetimage:latest",
        "--volume=storage://mycluster/myproject/.builds/mocked-uuid-4/.docker.config.json:/kaniko/.docker/config.json:ro",  # noqa: E501
        "--volume=storage://mycluster/myproject/context:/kaniko_context:rw",
        "--env=container=docker",
        "gcr.io/kaniko-project/executor:v1.20.0-debug",
    ]
    assert start_build_kaniko_args == [
        "--context=/kaniko_context",
        "--dockerfile=/kaniko_context/path/to/Dockerfile",
        "--destination=registry.mycluster.noexists/myproject/targetimage:latest",
        "--cache=true",
        "--cache-repo=registry.mycluster.noexists/myproject/layer-cache/cache",
        "--verbosity=info",
        "--image-fs-extract-retry=1",
        "--push-retry=3",
        "--use-new-run=true",
        "--snapshot-mode=redo",
    ]
