import asyncio

from neuromation import api as neuro_api
from neuromation.api.url_utils import uri_from_cli
from yarl import URL

from neuro_extras.cli import NEURO_EXTRAS_IMAGE


async def _copy_storage(source: str, destination: str) -> None:
    src_uri = uri_from_cli(source, "", "")
    src_cluster = src_uri.host
    src_path = src_uri.parts[2:]

    dst_uri = uri_from_cli(destination, "", "")
    dst_cluster = dst_uri.host
    dst_path = dst_uri.parts[2:]

    assert src_cluster
    assert dst_cluster
    async with neuro_api.get() as client:
        await client.config.switch_cluster(dst_cluster)
        await client.storage.mkdir(URL("storage:"), parents=True, exist_ok=True)
    await _run_copy_container(src_cluster, "/".join(src_path), "/".join(dst_path))


async def _run_copy_container(src_cluster: str, src_path: str, dst_path: str) -> None:
    args = [
        "neuro",
        "run",
        "-s",
        "cpu-small",
        "--pass-config",
        "-v",
        "storage:://storage",
        "-e",
        f"NEURO_CLUSTER={src_cluster}",
        NEURO_EXTRAS_IMAGE,
        f'"neuro cp --progress -r -u -T storage:{src_path} /storage/{dst_path}"',
    ]
    cmd = " ".join(args)
    print(f"Executing '{cmd}'")
    subprocess = await asyncio.create_subprocess_shell(cmd)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise Exception("Unable to copy storage")
