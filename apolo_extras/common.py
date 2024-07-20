import asyncio
import logging
import os

import apolo_sdk
import click

from .const import EX_OK, EX_PLATFORMERROR
from .version import __version__


logger = logging.getLogger(__name__)

APOLO_EXTRAS_IMAGE = os.environ.get(
    "APOLO_EXTRAS_IMAGE", f"ghcr.io/neuro-inc/apolo-extras:{__version__}"
)


async def _attach_job_stdout(
    job: apolo_sdk.JobDescription, client: apolo_sdk.Client, name: str = ""
) -> int:
    while job.status == apolo_sdk.JobStatus.PENDING:
        job = await client.jobs.status(job.id)
        await asyncio.sleep(1.0)
    async for chunk in client.jobs.monitor(job.id):
        if not chunk:
            break
        click.echo(chunk.decode(errors="ignore"), nl=False)
    while job.status in (apolo_sdk.JobStatus.PENDING, apolo_sdk.JobStatus.RUNNING):
        job = await client.jobs.status(job.id)
        await asyncio.sleep(1.0)

    job = await client.jobs.status(job.id)
    exit_code = EX_PLATFORMERROR
    if job.status == apolo_sdk.JobStatus.SUCCEEDED:
        exit_code = EX_OK
    elif job.status == apolo_sdk.JobStatus.FAILED:
        logger.error(f"The {name} job {job.id} failed due to:")
        logger.error(f"  Reason: {job.history.reason}")
        logger.error(f"  Description: {job.history.description}")
        exit_code = job.history.exit_code or EX_PLATFORMERROR  # never 0 for failed
    elif job.status == apolo_sdk.JobStatus.CANCELLED:
        logger.error(f"The {name} job {job.id} was cancelled")
    else:
        logger.error(f"The {name} job {job.id} terminated, status: {job.status}")
    return exit_code
