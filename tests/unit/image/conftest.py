import time
import typing as t
from contextlib import ExitStack
from decimal import Decimal
from unittest import mock

import apolo_sdk
import pytest
from apolo_sdk._config import _AuthConfig, _AuthToken, _ConfigData
from jose import jwt
from yarl import URL

from apolo_extras.image_builder import ImageBuilder


def _get_mock_presets() -> t.Dict[str, apolo_sdk.Preset]:
    return {
        "cpu-small": apolo_sdk.Preset(
            credits_per_hour=Decimal(1), cpu=1, memory=1 * 1024 * 1024
        ),
        "custom-preset": apolo_sdk.Preset(
            credits_per_hour=Decimal(1),
            cpu=5,
            memory=3 * 1024 * 1024,
            nvidia_gpu=1,
        ),
    }


def _get_mock_clusters() -> t.Dict[str, apolo_sdk.Cluster]:
    return {
        "mycluster": apolo_sdk.Cluster(
            name="mycluster",
            registry_url=URL("https://registry.mycluster.noexists"),
            storage_url=URL("https://mycluster.noexists/api/v1/storage"),
            users_url=URL("https://noexists/api/v1/users"),
            monitoring_url=URL("https://mycluster.noexists/api/v1/jobs"),
            secrets_url=URL("https://mycluster.noexists/api/v1/secrets"),
            disks_url=URL("https://mycluster.noexists/api/v1/disks"),
            buckets_url=URL("https://mycluster.noexists/api/v1/buckets"),
            resource_pools={},
            presets=_get_mock_presets(),
            orgs=[],
        ),
    }


def _get_mock_projects() -> t.Dict[apolo_sdk.Project.Key, apolo_sdk.Project]:
    return {
        apolo_sdk.Project.Key(
            cluster_name="mycluster", org_name=None, project_name="myproject"
        ): apolo_sdk.Project(
            cluster_name="mycluster", org_name=None, name="myproject", role="admin"
        ),
    }


def _load_mock_sdk_config() -> _ConfigData:
    return _ConfigData(
        _AuthConfig(
            auth_url=URL("https://notexists/login"),
            token_url=URL("https://notexists/token"),
            logout_url=URL("https://notexists/logout"),
            client_id="myclientid",
            audience="myaudience",
            headless_callback_url=URL("https://notexists/callback"),
        ),
        auth_token=_AuthToken(
            token=jwt.encode({"identity": "myusername"}, "secret"),
            expiration_time=time.time() + 1000,
            refresh_token="myrefreshtoken",
        ),
        url=URL("https://notexists/v1/api"),
        admin_url=URL("https://notexists/api/v1/admin"),
        version="1.0.0",
        project_name="myproject",
        cluster_name="mycluster",
        org_name=None,
        clusters=_get_mock_clusters(),
        projects=_get_mock_projects(),
    )


class MockedApoloConfig(apolo_sdk.Config):
    def _load(self) -> _ConfigData:
        ret = self.__config_data = _load_mock_sdk_config()
        return ret

    async def check_server(self) -> None:
        pass


@pytest.fixture
async def _apolo_client() -> t.AsyncGenerator[apolo_sdk.Client, None]:
    with ExitStack() as stack:
        stack.enter_context(mock.patch("apolo_sdk.Config", MockedApoloConfig))
        stack.enter_context(mock.patch("apolo_sdk._client.Config", MockedApoloConfig))

        stack.enter_context(
            mock.patch("apolo_sdk._storage.Storage.mkdir", mock.AsyncMock())
        )
        stack.enter_context(
            mock.patch("apolo_sdk._storage.Storage.create", mock.AsyncMock())
        )
        stack.enter_context(
            mock.patch("apolo_extras.image._check_image_exists", return_value=False)
        )
        stack.enter_context(mock.patch("uuid.uuid4", return_value="mocked-uuid-4"))
        client = await apolo_sdk.get()
        try:
            yield await client.__aenter__()
        finally:
            await client.__aexit__()


@pytest.fixture
def remote_image_builder(_apolo_client: apolo_sdk.Client) -> ImageBuilder:
    builder_class = ImageBuilder.get(local=False)
    builder_class._execute_subprocess = mock.AsyncMock(  # type: ignore
        side_effect=lambda x: 0
    )

    return builder_class(client=_apolo_client)
