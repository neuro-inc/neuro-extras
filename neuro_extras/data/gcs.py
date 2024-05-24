"""Module for copying files from/to Google Cloud Storage"""

from ..utils import CLIRunner
from .common import Copier, DataUrlType, Resource


class GCSCopier(Copier, CLIRunner):
    """Copier, that is capable of copying to/from Google Cloud Storage"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source.data_url_type == DataUrlType.LOCAL_FS
            and self.destination.data_url_type == DataUrlType.GCS
            or self.source.data_url_type == DataUrlType.GCS
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError(
                "Unsupported source and destination - "
                f"can only copy between {DataUrlType.GCS.name} "
                f"and {DataUrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> Resource:
        """Perform copy through running gsutil and return the url to destinaton"""

        command = "gsutil"
        args = ["-m", "cp", "-r", str(self.source.url), str(self.destination.url)]
        await self.run_command(command=command, args=args)
        return self.destination
