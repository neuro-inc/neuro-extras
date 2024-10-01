import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import AsyncIterator, List, Optional

import apolo_sdk
from apolo_sdk import Client


logger = logging.getLogger(__name__)


class CLIRunner:
    """Utility class for running shell commands"""

    async def run_command(self, command: str, args: List[str]) -> None:
        """Execute command with args

        If resulting statuscode is non-zero, RuntimeError is thrown
        with stderr as a message.
        """
        logger.info(f"Executing: {[command] + args}")
        # logger.warn(f"Calling echo instead of actual command!")
        # process = await asyncio.create_subprocess_exec("echo", *([command] + args))

        process = await asyncio.create_subprocess_exec(command, *args)
        status_code = await process.wait()
        if status_code != 0:
            raise RuntimeError(process.stderr)


@asynccontextmanager
async def get_platform_client(
    cluster: Optional[str] = None,
) -> AsyncIterator[apolo_sdk.Client]:
    client: apolo_sdk.Client = await apolo_sdk.get()
    cluster_orig: Optional[str] = None
    try:
        await client.__aenter__()

        cluster_orig = client.cluster_name
        if cluster is not None:
            if cluster != cluster_orig:
                logger.info(
                    f"Temporarily switching cluster: {cluster_orig} -> {cluster}"
                )
                await client.config.switch_cluster(cluster)  # typing: ignore
            else:
                logger.info(f"Already on cluster: {cluster}")
        yield client
    finally:
        # NOTE: bypass https://github.com/neuro-inc/platform-client-python/issues/1816
        try:
            await client.__aexit__(None, None, None)
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            logger.warning(f"Ignoring exception during closing apolo client: {e}")

        if cluster is not None and cluster_orig is not None and cluster != cluster_orig:
            logger.info(f"Switching back cluster: {cluster} -> {cluster_orig}")
            try:
                await client.config.switch_cluster(cluster_orig)
            except asyncio.CancelledError:
                raise
            except BaseException:
                logger.error(
                    f"Could not switch back to cluster '{cluster_orig}'. Please "
                    f"run manually: 'apolo config switch-cluster {cluster_orig}'"
                )


def select_job_preset(
    preset: Optional[str], client: Client, min_cpu: float = 2, min_mem: int = 4096
) -> Optional[str]:
    """
    Try to automatically select the best available preset for a task.
    Memory is specified in mebibytes.
    """
    good_presets = []
    good_presets_names = []
    # Build a shortlist of presets that could fit
    for cluster_preset_name, cluster_preset_info in client.presets.items():
        # Don't even try to use GPU machines for image builds
        # Also ignore scheduled presets (they don't work with schedule-timeout)
        # see https://github.com/neuro-inc/neuro-extras/issues/488
        if (
            cluster_preset_info.cpu >= min_cpu
            and cluster_preset_info.memory_mb >= min_mem
            and not cluster_preset_info.scheduler_enabled
        ):
            good_presets.append((cluster_preset_name, cluster_preset_info))
            good_presets_names.append(cluster_preset_name)
    # Sort presets by cost - memory - cpu
    good_presets.sort(key=lambda p: (p[1].credits_per_hour, p[1].memory_mb, p[1].cpu))

    if preset is None:
        if len(good_presets) > 0:
            # Select the best preset
            preset_name = good_presets[0][0]
            logger.info(f"Automatically selected build preset {preset_name}")
            return preset_name
        else:
            # Fallback to the default preset selection mechanism by apolo sdk
            logger.warning(
                "No resource preset satisfying minimal hardware "
                "requirements was found in the cluster. "
                "The job might take long time to accomplish. "
                "Consider contacting your cluster manager or admin "
                "to adjust the cluster configuration"
            )
            return None
    else:
        if preset in good_presets_names:
            # If user asked for a preset, and it's a good one - let them use it
            return preset
        else:
            if len(good_presets) > 0:
                # We have a better preset
                logger.warning(
                    f"The selected resource preset {preset} does not "
                    f"satisfy recommended minimum hardware requirements. "
                    f"Consider using '{good_presets[0][0]}'"
                )
            return preset


def get_default_preset(apolo_client: Client) -> str:
    """Get default preset name via Neu.ro client"""
    return next(iter(apolo_client.presets.keys()))


def provide_temp_dir(
    dir: Path = Path.home() / ".apolo-tmp",
) -> TemporaryDirectory:  # type: ignore
    """Provide temp directory"""
    dir.mkdir(exist_ok=True, parents=True)
    return TemporaryDirectory(dir=dir)
