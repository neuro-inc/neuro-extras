"""Module for copying files from/to S3"""

from ..utils import CLIRunner
from .common import Copier, DataUrlType, Resource


class S3Copier(Copier, CLIRunner):
    """Copier, that is capable of copying to/from Amazon S3"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source.data_url_type == DataUrlType.LOCAL_FS
            and self.destination.data_url_type == DataUrlType.S3
            or self.source.data_url_type == DataUrlType.S3
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {DataUrlType.S3.name} "
                f"and {DataUrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> Resource:
        """Perform copy through running aws cli and return the url to destinaton"""

        command = "aws"
        if self.source.as_str().endswith("/"):
            args = [
                "s3",
                "cp",
                "--recursive",
                self.source.as_str(),
                self.destination.as_str(),
            ]
        else:
            args = ["s3", "cp", self.source.as_str(), self.destination.as_str()]
        await self.run_command(command=command, args=args)
        return self.destination
