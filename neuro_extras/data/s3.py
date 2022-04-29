import logging

from .common import CLICopier, UrlType


logger = logging.getLogger(__name__)


class S3Copier(CLICopier):
    """Copier, that is capable of copying to/from Amazon S3"""

    async def perform_copy(self) -> str:
        if UrlType.S3 not in (self.source_type, self.destination_type):
            raise ValueError(
                "Unsupported source and destination - "
                "at least one should start with s3://"
            )
        command = "aws"
        if self.source.endswith("/"):
            args = ["s3", "cp", "--recursive", self.source, self.destination]
        else:
            args = ["s3", "cp", self.source, self.destination]
        await self.run_command(command=command, args=args)
        return self.destination
