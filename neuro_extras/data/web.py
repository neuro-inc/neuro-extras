"""Module for copying files from HTTP(S) sources"""
from yarl import URL

from ..utils import CLIRunner
from .common import Copier, UrlType


class WebCopier(Copier, CLIRunner):
    """Copier for downloading data from HTTP(S) sources"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.WEB
            and self.destination_type == UrlType.LOCAL_FS
        ):
            raise ValueError(
                f"Can only copy from {UrlType.WEB.name} to {UrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> str:
        """Perform copy through running rclone and return the url to destinaton"""
        command = "rclone"
        source_url = URL(self.source)
        args = [
            "copyto",
            "--http-url",
            # HTTP URL parameter for rclone is just scheme + host name
            str(source_url.with_path("").with_query("")),
            f":http:{source_url.path}",
            self.destination,
        ]
        await self.run_command(command=command, args=args)
        return self.destination
