"""Module for copying files from/to Azure"""

import logging
import os
from urllib import parse

from yarl import URL

from ..utils import CLIRunner
from .common import Copier, DataUrlType, Resource


logger = logging.getLogger(__name__)


class AzureCopier(Copier, CLIRunner):
    """Copier, that is capable of copying to/from Azure storage"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source.data_url_type == DataUrlType.LOCAL_FS
            and self.destination.data_url_type == DataUrlType.AZURE
            or self.source.data_url_type == DataUrlType.AZURE
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {DataUrlType.AZURE.name} "
                f"and {DataUrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> Resource:
        """Perform copy from self.source into self.destination through rclone
        and return url to the copied resource"""

        sas_url_source = (
            self.source
            if self.source.data_url_type == DataUrlType.AZURE
            else self.destination
        )
        sas_url = _build_sas_url(sas_url_source.url)
        source = _patch_azure_url_for_rclone(self.source.url)
        destination = _patch_azure_url_for_rclone(self.destination.url)
        command = "rclone"
        args = ["copyto", "-v", "--azureblob-sas-url", sas_url, source, destination]
        await self.run_command(command=command, args=args)
        return self.destination


def _build_sas_url(azure_url: URL) -> str:
    """
    In order to build SAS URL we replace original URL scheme with HTTPS,
    remove everything from path except bucket name and append SAS token as a query
    """
    sas_token = os.getenv("AZURE_SAS_TOKEN", "")
    if not sas_token:
        logger.warning("AZURE_SAS_TOKEN env is not provided")
    quoted_url = (
        azure_url.with_scheme("https")
        .with_path("/".join(azure_url.path.split("/")[:2]))
        .with_query(sas_token)
    )
    # with_query performs urlencode of sas_token, which breaks the token,
    # so we urldecode the resulting url
    sas_url = parse.unquote(str(quoted_url))
    logger.debug(f"SAS URL: {sas_url}")
    return sas_url


def _patch_azure_url_for_rclone(url: URL) -> str:
    """Replace host part with :azureblob: if the url is of azure type"""
    if url.scheme == "azure+https":
        return f":azureblob:{url.path}"
    else:
        return str(url)
