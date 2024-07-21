"""Module for copying files on local filesystem"""

import os
from pathlib import Path

from ..utils import CLIRunner
from .common import Copier, DataUrlType, Resource


class LocalFSCopier(Copier, CLIRunner):
    """Copier implementation for local file system operations"""

    def _ensure_can_execute(self) -> None:
        if not (
            self.source.data_url_type == DataUrlType.LOCAL_FS
            and self.destination.data_url_type == DataUrlType.LOCAL_FS
        ):
            raise ValueError("Only local filesystem is supported")

    async def perform_copy(self) -> Resource:
        """Perform copy through running rclone and return the url to destinaton"""
        destination_path = Path(self.destination.url.path)
        destination_parent_folder, _ = os.path.split(destination_path)
        Path(destination_parent_folder).mkdir(exist_ok=True, parents=True)
        command = "rclone"
        args = [
            "copyto",  # TODO: investigate usage of 'sync' for potential speedup.
            "--checkers=16",  # https://rclone.org/docs/#checkers-n , default is 8
            "--transfers=8",  # https://rclone.org/docs/#transfers-n , default is 4.
            "--verbose=1",  # default is 0, set 2 for debug
            self.source.as_str(),
            self.destination.as_str(),
        ]
        await self.run_command(command=command, args=args)
        return self.destination
