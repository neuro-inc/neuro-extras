import logging

from .common import CLICopier


logger = logging.getLogger(__name__)


class GCSCopier(CLICopier):
    """Copier, that is capable of copying to/from Google Cloud Storage"""

    async def perform_copy(self) -> str:
        command = "gsutil"
        args = ["-m", "cp", "-r", self.source, self.destination]
        await self.run_command(command=command, args=args)
        return self.destination
