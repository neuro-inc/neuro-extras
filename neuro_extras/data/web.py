"""Module for copying files from HTTP(S) sources"""

from ..utils import CLIRunner
from .common import Copier, DataUrlType, Resource


class WebCopier(Copier, CLIRunner):
    """Copier for downloading data from HTTP(S) sources"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source.data_url_type == DataUrlType.WEB
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError(
                f"Can only copy from {DataUrlType.WEB.name} "
                f"to {DataUrlType.LOCAL_FS.name}"
            )

    async def perform_copy(self) -> Resource:
        """Perform copy through running rclone and return the url to destinaton"""
        if not self.source.data_url_type == DataUrlType.WEB:
            raise ValueError("Only copy from HTTP(s) sources is supported")
        if self.destination.data_url_type == DataUrlType.WEB:
            raise ValueError("Copy to HTTP(S) destinations is unsupported")
        if self.source.filename is None:
            raise ValueError(
                "Copy from HTTP(S) directory is unsupported. "
                "Please, reach us at https://github.com/neuro-inc/neuro-extras/issues "
                "describing your use case."
            )
        command = "rclone"
        args = [
            "copyto",
            "--http-url",
            # HTTP URL parameter for rclone is just scheme + host name
            str(self.source.url.with_path("").with_query("")),
            f":http:{self.source.url.path}",
            self.destination.as_str(),
        ]
        await self.run_command(command=command, args=args)
        return self.destination
