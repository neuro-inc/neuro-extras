"""Module for copying files from/to S3"""

from .common import CLIRunner, Copier, UrlType


class S3Copier(Copier, CLIRunner):
    """Copier, that is capable of copying to/from Amazon S3"""

    async def perform_copy(self) -> str:
        """Perform copy through running aws cli and return the url to destinaton"""
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
