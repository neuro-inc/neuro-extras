import logging

from .common import CLICopier, UrlType


logger = logging.getLogger(__name__)


class GCSCopier(CLICopier):
    """Copier, that is capable of copying to/from Google Cloud Storage"""

    async def perform_copy(self) -> str:
        if UrlType.GCS not in (self.source_type, self.destination_type):
            raise ValueError(
                "Unsupported source and destination - "
                "at least one should start with gs://"
            )
        command = "gsutil"
        args = ["-m", "cp", "-r", self.source, self.destination]
        await self.run_command(command=command, args=args)
        return self.destination
