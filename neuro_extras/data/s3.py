"""Module for copying files from/to S3"""

from ..utils import CLIRunner
from .common import Copier, UrlType


class S3Copier(Copier, CLIRunner):
    """Copier, that is capable of copying to/from Amazon S3"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.LOCAL_FS
            and self.destination_type == UrlType.S3
            or self.source_type == UrlType.S3
            and self.destination_type == UrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {UrlType.S3.name} "
                f"and {UrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> str:
        """Perform copy through running aws cli and return the url to destinaton"""

        command = "aws"
        if self.source.endswith("/"):
            args = ["s3", "cp", "--recursive", self.source, self.destination]
        else:
            args = ["s3", "cp", self.source, self.destination]
        await self.run_command(command=command, args=args)
        return self.destination
