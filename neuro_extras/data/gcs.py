"""Module for copying files from/to Google Cloud Storage"""
from ..utils import CLIRunner
from .common import Copier, UrlType


class GCSCopier(Copier, CLIRunner):
    """Copier, that is capable of copying to/from Google Cloud Storage"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.LOCAL_FS
            and self.destination_type == UrlType.GCS
            or self.source_type == UrlType.GCS
            and self.destination_type == UrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {UrlType.GCS.name} "
                f"and {UrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> str:
        """Perform copy through running gsutil and return the url to destinaton"""

        command = "gsutil"
        args = ["-m", "cp", "-r", self.source, self.destination]
        await self.run_command(command=command, args=args)
        return self.destination
