import asyncio
import logging
import sys
from typing import AsyncIterator, Optional

import neuro_sdk as neuro_api


if sys.version_info >= (3, 7):  # pragma: no cover
    from contextlib import asynccontextmanager
else:
    from async_generator import asynccontextmanager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_neuro_client(
    cluster: Optional[str] = None,
) -> AsyncIterator[neuro_api.Client]:
    client: neuro_api.Client = await neuro_api.get()
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
            logger.warning(f"Ignoring exception during closing neuro client: {e}")

        if cluster is not None and cluster_orig is not None and cluster != cluster_orig:
            logger.info(f"Switching back cluster: {cluster} -> {cluster_orig}")
            try:
                await client.config.switch_cluster(cluster_orig)
            except asyncio.CancelledError:
                raise
            except BaseException:
                logger.error(
                    f"Could not switch back to cluster '{cluster_orig}'. Please "
                    f"run manually: 'neuro config switch-cluster {cluster_orig}'"
                )
