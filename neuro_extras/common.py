import asyncio
import logging
import os

import click
import neuro_sdk

from .const import EX_OK, EX_PLATFORMERROR
from .version import __version__


logger = logging.getLogger(__name__)

NEURO_EXTRAS_IMAGE = os.environ.get(
    "NEURO_EXTRAS_IMAGE", f"ghcr.io/neuro-inc/neuro-extras:{__version__}"
)


async def _attach_job_stdout(
    job: neuro_sdk.JobDescription, client: neuro_sdk.Client, name: str = ""
) -> int:
    while job.status == neuro_sdk.JobStatus.PENDING:
        job = await client.jobs.status(job.id)
        await asyncio.sleep(1.0)
    async for chunk in client.jobs.monitor(job.id):
        if not chunk:
            break
        click.echo(chunk.decode(errors="ignore"), nl=False)
    while job.status in (neuro_sdk.JobStatus.PENDING, neuro_sdk.JobStatus.RUNNING):
        job = await client.jobs.status(job.id)
        await asyncio.sleep(1.0)

    job = await client.jobs.status(job.id)
    exit_code = EX_PLATFORMERROR
    if job.status == neuro_sdk.JobStatus.SUCCEEDED:
        exit_code = EX_OK
    elif job.status == neuro_sdk.JobStatus.FAILED:
        logger.error(f"The {name} job {job.id} failed due to:")
        logger.error(f"  Reason: {job.history.reason}")
        logger.error(f"  Description: {job.history.description}")
        exit_code = job.history.exit_code or EX_PLATFORMERROR  # never 0 for failed
    elif job.status == neuro_sdk.JobStatus.CANCELLED:
        logger.error(f"The {name} job {job.id} was cancelled")
    else:
        logger.error(f"The {name} job {job.id} terminated, status: {job.status}")
    return exit_code
