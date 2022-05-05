"""Module for copying files from HTTP(S) sources"""
from yarl import URL

from .common import CLIRunner, Copier, UrlType


class WebCopier(Copier, CLIRunner):
    """Copier for downloading data from HTTP(S) sources"""

    async def perform_copy(self) -> str:
        """Perform copy through running rclone and return the url to destinaton"""
        if not self.source_type == UrlType.WEB:
            raise ValueError("Only copy from HTTP(s) sources is supported")
        if self.destination_type == UrlType.WEB:
            raise ValueError("Copy to HTTP(S) destinations is unsupported")
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
