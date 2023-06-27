import dataclasses
import logging
import re
from pathlib import Path
from time import sleep
from typing import List, Optional

from neuro_sdk import Client
from yarl import URL

from neuro_extras.data.azure import _build_sas_url, _patch_azure_url_for_rclone
from neuro_extras.data.common import Resource

from ..conftest import DISK_PREFIX, TEMPDIR_PREFIX
from .utils import _run_command


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DataTestResource:
    schema: str
    url: str
    is_archive: bool
    file_extension: Optional[str] = None
    client: Optional[Client] = None

    def remove(self) -> None:
        """Remove the resource at self.url"""
        removal_handlers = {
            "local": _remove_local,
            "s3": _remove_s3,
            "azure+https": _remove_azure,
            "gs": _remove_gcs,
            "storage": _remove_storage,
        }
        if self.schema in removal_handlers:
            logger.info(f"Removing {self.url}")
            removal_handlers[self.schema](self.url)
        else:
            logger.warning(f"No cleanup handler for schema '{self.schema}'")

    def exists(self) -> bool:
        """Check if the resource exists and is of appropriate type (dir or file)"""
        handlers = {
            "local": _local_resource_exists,
            "s3": _s3_resource_exists,
            "gs": _gcs_resource_exists,
            "azure+https": _azure_resource_exist,
            "storage": _storage_resource_exists,
            "disk": _resource_on_disk_exists,
        }
        if self.schema in handlers:
            logger.info(f"Checking if {self.url} exists")
            return handlers[self.schema](self)
        else:
            logger.error(f"Check for '{self.schema}' schema is not implemented!")
            return False

    def patch_disk(self, disk: str) -> None:
        """Patch disk url containing DISK_PREFIX with specified disk id"""
        if self.schema == "disk":
            patched = re.sub(DISK_PREFIX, disk, self.url)
            logger.info(f"Patched {self.url} into {patched}")
            self.url = patched

    def patch_tempdir(self, dir: str) -> None:
        if self.schema == "local":
            patched = re.sub(TEMPDIR_PREFIX, dir, self.url)
            logger.info(f"Patched {self.url} into {patched}")
            self.url = patched


def _remove_local(url: str) -> None:
    returncode, stdout, stderr = _run_command("rm", ["-rvf", url])
    assert returncode == 0, stderr


def _remove_gcs(url: str) -> None:
    returncode, stdout, stderr = _run_command("gsutil", ["-m", "rm", "-r", url])
    assert returncode == 0, stderr


def _remove_s3(url: str) -> None:
    returncode, stdout, stderr = _run_command("aws", ["s3", "rm", "--recursive", url])
    assert returncode == 0, stderr


def _remove_storage(url: str) -> None:
    returncode, stdout, stderr = _run_command("neuro", ["storage", "rm", "-r", url])
    assert returncode == 0 or "Not Found" in stderr


def _remove_azure(url: str) -> None:
    azure_sas_url = _build_sas_url(URL(url))
    patched_url = _patch_azure_url_for_rclone(URL(url))
    returncode, stdout, stderr = _run_command(
        "rclone", ["delete", "--azureblob-sas-url", azure_sas_url, patched_url]
    )
    assert returncode == 0, stderr


def _local_resource_exists(resource: DataTestResource) -> bool:
    path = Path(resource.url)
    path_exists = path.exists()
    valid_type = (
        path.is_file() if resource.file_extension is not None else path.is_dir()
    )
    return path_exists and valid_type


def _s3_resource_exists(resource: DataTestResource) -> bool:
    if resource.file_extension:
        url = URL(resource.url)
        bucket_name = url.host
        filename = "/".join(url.parts[1:])
        command = "aws"
        args = ["s3api", "head-object", "--bucket", bucket_name, "--key", filename]
    else:
        command = "aws"
        args = [
            "s3",
            "ls",
            resource.url,
        ]
    returcode, stdout, stederr = _run_command(command, args)  # type: ignore
    return returcode == 0


def _storage_resource_exists(resource: DataTestResource) -> bool:
    sleep(5)  # (A.K.) I hate to admit it, but without this it often does not work
    command = "neuro"
    assert resource.client is not None
    if resource.file_extension is None:
        args = [
            "storage",
            "ls",
            "-l",
            Resource.parse(resource.url, client=resource.client).as_str(),
        ]
    else:
        args = [
            "storage",
            "ls",
            "-l",
            Resource.parse(resource.url, client=resource.client)
            .strip_filename()
            .as_str(),
        ]
    returncode, stdout, stderr = _run_command(command, args)
    if resource.file_extension is None:
        check_is_successful = True
    else:
        stdout = stdout if stdout else ""
        filename = Resource.parse(resource.url, client=resource.client).filename
        assert filename is not None
        check_is_successful = filename in stdout
    return returncode == 0 and check_is_successful


def _resource_on_disk_exists(resource: DataTestResource) -> bool:
    command = "neuro"
    assert resource.client
    disk_id, path_on_disk = Resource.parse(
        resource.url, client=resource.client
    ).disk_id_and_path
    mountpoint = "/var/mnt"
    path_in_job = f"{mountpoint}{path_on_disk if path_on_disk else '/'}"
    args = [
        "job",
        "run",
        "-v",
        f"{disk_id}:{mountpoint}",
        "busybox",
        "--",
        "stat",
        path_in_job,
    ]
    returncode, stdout, stderr = _run_command(command, args)
    return returncode == 0


def _gcs_resource_exists(resource: DataTestResource) -> bool:
    command = "gsutil"
    args = [
        "ls",
        resource.url,
    ]
    returncode, _, _ = _run_command(command, args)
    return returncode == 0


def _azure_resource_exist(resource: DataTestResource) -> bool:
    command = "rclone"
    args = [
        "-q",
        "--azureblob-sas-url",
        _build_sas_url(URL(resource.url)),
        "lsf",
        _patch_azure_url_for_rclone(URL(resource.url)),
    ]
    returncode, stdout, stderr = _run_command(command, args)
    return returncode == 0 and bool(stdout)


@dataclasses.dataclass
class CopyTestConfig:
    source: DataTestResource
    destination: DataTestResource
    extract_flag: bool = False
    compress_flag: bool = False
    # if set, force testcase to be expected to fail
    should_fail: Optional[bool] = None
    # if should_fail is set - provide reason
    fail_reason: Optional[str] = None

    def as_command(self, minimized: bool = False) -> List[str]:
        if minimized:
            command = []
        else:
            command = ["data", "cp"] + self.get_extra_args()
        if self.extract_flag:
            command.append("-x")
        if self.compress_flag:
            command.append("-c")
        if minimized:
            source_filepart = (
                self.source.file_extension if self.source.file_extension else "/"
            )
            source_url = f"{self.source.schema}:***{source_filepart}"
            destination_filepart = (
                self.destination.file_extension
                if self.destination.file_extension
                else "/"
            )
            destination_url = f"{self.destination.schema}:***{destination_filepart}"
        else:
            source_url = self.source.url
            destination_url = self.destination.url
        command += [
            source_url,
            destination_url,
        ]
        return command

    def __str__(self) -> str:
        return " ".join(self.as_command(minimized=True))

    def get_extra_args(self) -> List[str]:
        schemas = (self.source.schema, self.destination.schema)
        if not (("storage" in schemas) or ("disk" in schemas)):
            return []
        extra_args = []
        if "gs" in schemas:
            extra_args += [
                "-v",
                "secret:neuro-extras-gcp:/gcp-creds.txt",
                "-e",
                "GOOGLE_APPLICATION_CREDENTIALS=/gcp-creds.txt",
            ]
        if "azure+https" in schemas:
            extra_args += ["-e", "AZURE_SAS_TOKEN=secret:azure_sas_token"]
        if "s3" in schemas:
            extra_args += [
                "-v",
                "secret:neuro-extras-aws:/aws-creds.txt",
                "-e",
                "AWS_CONFIG_FILE=/aws-creds.txt",
            ]
        extra_args += ["--preset", "cpu-small"]
        return extra_args
