import logging
import os
from urllib import parse

from yarl import URL

from .common import CLICopier, UrlType


logger = logging.getLogger(__name__)


class AzureCopier(CLICopier):
    """Copier, that is capable of copying to/from Azure storage"""

    @staticmethod
    def build_sas_url(raw_url: str) -> str:
        """
        In order to build SAS URL we replace original URL scheme with HTTPS,
        remove everything from path except bucket name and append SAS token as a query
        """
        sas_token = os.getenv("AZURE_SAS_TOKEN", "")
        if not sas_token:
            logger.warn("AZURE_SAS_TOKEN env is not provided")
        azure_url = URL(raw_url)
        quoted_url = (
            azure_url.with_scheme("https")
            .with_path("/".join(azure_url.path.split("/")[:2]))
            .with_query(sas_token)
        )
        # with_query performs urlencode of sas_token, which breaks the token,
        # so we urldecode the resulting url
        sas_url = parse.unquote(str(quoted_url))
        logger.info(f"SAS URL: {sas_url}")
        return sas_url

    @staticmethod
    def patch_azure_url_for_rclone(raw_url: str) -> str:
        url = URL(raw_url)
        if url.scheme == "azure+https":
            return f":azureblob:{url.path}"
        else:
            return raw_url

    async def perform_copy(self) -> str:
        sas_url_source = (
            self.source if self.source_type == UrlType.AZURE else self.destination
        )
        sas_url = AzureCopier.build_sas_url(sas_url_source)
        source = AzureCopier.patch_azure_url_for_rclone(self.source)
        destination = AzureCopier.patch_azure_url_for_rclone(self.destination)
        command = "rclone"
        args = ["copyto", "--azureblob-sas-url", sas_url, source, destination]
        await self.run_command(command=command, args=args)
        return self.destination
