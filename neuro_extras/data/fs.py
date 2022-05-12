"""Module for copying files on local filesystem"""
from ..utils import CLIRunner
from .common import Copier, UrlType


class LocalFSCopier(Copier, CLIRunner):
    """Copier implementation for local file system operations"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source_type == UrlType.LOCAL_FS
            and self.destination_type == UrlType.LOCAL_FS
        ):
            raise ValueError("Only local filesystem is supported")

    async def perform_copy(self) -> str:
        """Perform copy through running rclone and return the url to destinaton"""
        command = "rclone"
        args = [
            "copyto",  # TODO: investigate usage of 'sync' for potential speedup.
            "--checkers=16",  # https://rclone.org/docs/#checkers-n , default is 8
            "--transfers=8",  # https://rclone.org/docs/#transfers-n , default is 4.
            "--verbose=1",  # default is 0, set 2 for debug
            self.source,
            self.destination,
        ]
        await self.run_command(command=command, args=args)
        return self.destination
