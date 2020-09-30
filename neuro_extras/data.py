import asyncio
import sys
import tempfile
from distutils import dir_util
from enum import Enum
from pathlib import Path
from typing import Sequence

import click
from neuromation import api as neuro_api
from neuromation.cli.const import EX_PLATFORMERROR
from yarl import URL

from neuro_extras.main import NEURO_EXTRAS_IMAGE, logger


TEMP_UNPACK_DIR = Path.home() / ".neuro-tmp"


class DataCopier:
    def __init__(self, client: neuro_api.Client):
        self._client = client

    async def launch(
        self,
        storage_uri: URL,
        extract: bool,
        src_uri: URL,
        dst_uri: URL,
        volume: Sequence[str],
        env: Sequence[str],
    ) -> neuro_api.JobDescription:
        logger.info("Submitting a copy job")
        copier_container = await self._create_copier_container(
            storage_uri, extract, src_uri, dst_uri, volume, env
        )
        job = await self._client.jobs.run(copier_container, life_span=60 * 60)
        logger.info(f"The copy job ID: {job.id}")
        return job

    async def _create_copier_container(
        self,
        storage_uri: URL,
        extract: bool,
        src_uri: URL,
        dst_uri: URL,
        volume: Sequence[str],
        env: Sequence[str],
    ) -> neuro_api.Container:
        args = f"{str(src_uri)} {str(dst_uri)}"
        if extract:
            args = f"-x {args}"

        env_dict, secret_env_dict = self._client.parse.env(env)
        vol = self._client.parse.volumes(volume)
        volumes, secret_files = list(vol.volumes), list(vol.secret_files)
        volumes.append(neuro_api.Volume(storage_uri, "/var/storage"))

        gcp_env = "GOOGLE_APPLICATION_CREDENTIALS"
        cmd = (
            f'( [ "${gcp_env}" ] && '
            f"gcloud auth activate-service-account --key-file ${gcp_env} ) ; "
            f"neuro-extras data cp {args}"
        )
        return neuro_api.Container(
            image=neuro_api.RemoteImage.new_external_image(NEURO_EXTRAS_IMAGE),
            resources=neuro_api.Resources(cpu=2.0, memory_mb=4096),
            volumes=volumes,
            command=f"bash -c '{cmd} '",
            env=env_dict,
            secret_env=secret_env_dict,
            secret_files=secret_files,
        )


class UrlType(Enum):
    UNSUPPORTED = 0
    LOCAL = 1
    CLOUD = 2
    STORAGE = 3

    @staticmethod
    def get_type(url: URL) -> "UrlType":
        if url.scheme == "storage":
            return UrlType.STORAGE
        if url.scheme == "":
            return UrlType.LOCAL
        if url.scheme in ("s3", "gs"):
            return UrlType.CLOUD
        return UrlType.UNSUPPORTED


async def _data_cp(
    source: str,
    destination: str,
    extract: bool,
    compress: bool,
    volume: Sequence[str],
    env: Sequence[str],
) -> None:
    source_url = URL(source)
    destination_url = URL(destination)
    source_url_type = UrlType.get_type(source_url)
    if source_url_type == UrlType.UNSUPPORTED:
        raise ValueError(f"Unsupported source URL scheme: {source_url.scheme}")
    destination_url_type = UrlType.get_type(destination_url)
    if destination_url_type == UrlType.UNSUPPORTED:
        raise ValueError(
            f"Unsupported destination URL scheme: {destination_url.scheme}"
        )

    if source_url_type == UrlType.CLOUD and destination_url_type == UrlType.CLOUD:
        raise ValueError(
            "This command can't be used to copy data between cloud providers"
        )
    if source_url_type == UrlType.STORAGE and destination_url_type == UrlType.STORAGE:
        raise ValueError(
            "This command can't be used to copy data between two storage locations"
        )

    if UrlType.STORAGE in (source_url_type, destination_url_type):
        async with neuro_api.get() as client:
            data_copier = DataCopier(client)
            if source_url_type == UrlType.STORAGE:
                job = await data_copier.launch(
                    storage_uri=source_url,
                    src_uri=URL("/var/storage"),
                    dst_uri=destination_url,
                    extract=extract,
                    volume=volume,
                    env=env,
                )
            else:
                job = await data_copier.launch(
                    storage_uri=destination_url,
                    src_uri=source_url,
                    dst_uri=URL("/var/storage"),
                    extract=extract,
                    volume=volume,
                    env=env,
                )
            while job.status == neuro_api.JobStatus.PENDING:
                job = await client.jobs.status(job.id)
                await asyncio.sleep(1.0)
            async for chunk in client.jobs.monitor(job.id):
                if not chunk:
                    break
                click.echo(chunk.decode(errors="ignore"), nl=False)
            job = await client.jobs.status(job.id)
            if job.status == neuro_api.JobStatus.FAILED:
                logger.error("The copy job has failed due to:")
                logger.error(f"  Reason: {job.history.reason}")
                logger.error(f"  Description: {job.history.description}")
                exit_code = job.history.exit_code
                if exit_code is None:
                    exit_code = EX_PLATFORMERROR
                sys.exit(exit_code)
            else:
                logger.info("Successfully copied data")
    else:
        TEMP_UNPACK_DIR.mkdir(parents=True, exist_ok=True)
        tmp_dir_name = tempfile.mkdtemp(dir=str(TEMP_UNPACK_DIR))
        tmp_dst_url = URL.build(path=(tmp_dir_name + "/"))

        await _nonstorage_cp(source_url, tmp_dst_url)
        source_url = tmp_dst_url
        # for clarity
        source_url_type = UrlType.LOCAL

        # at this point source is always local
        if extract:
            # extract to tmp dir
            file = Path(source_url.path)
            if file.is_dir():
                file = list(file.glob("*"))[0]
            suffixes = file.suffixes
            if suffixes[-2:] == [".tar", ".gz"] or suffixes[-1] == ".tgz":
                command = "tar"
                args = ["zxvf", str(file), "-C", str(tmp_dst_url)]
            elif suffixes[-2:] == [".tar", ".bz2"] or suffixes[-1] in (".tbz2", ".tbz"):
                command = "tar"
                args = ["jxvf", str(file), "-C", str(tmp_dst_url)]
            elif suffixes[-1] == ".tar":
                command = "tar"
                args = ["xvf", str(file), "-C", str(tmp_dst_url)]
            elif suffixes[-1] == ".gz":
                command = "gunzip"
                args = [str(file), str(tmp_dst_url) + file.name[:-3]]
            elif suffixes[-1] == ".zip":
                command = "unzip"
                args = [str(file), "-d", str(tmp_dst_url)]
            else:
                raise ValueError(f"Don't know how to extract file {file.name}")

            click.echo(f"Running {command} {' '.join(args)}")
            subprocess = await asyncio.create_subprocess_exec(command, *args)
            returncode = await subprocess.wait()
            if returncode != 0:
                raise click.ClickException(f"Extraction failed: {subprocess.stderr}")
            else:
                if file.exists():
                    # gunzip removes src after extraction, while tar - not
                    file.unlink()

        if compress:
            file = Path(destination_url.path.split("/")[-1])
            tmp_dst_archive = Path(TEMP_UNPACK_DIR / file)
            suffixes = file.suffixes
            if suffixes[-2:] == [".tar", ".gz"] or suffixes[-1] == ".tgz":
                command = "tar"
                args = ["zcf", str(tmp_dst_archive), "-C", str(source_url.path), "."]
            elif suffixes[-2:] == [".tar", ".bz2"] or suffixes[-1] in (".tbz2", ".tbz"):
                command = "tar"
                args = ["jcf", str(tmp_dst_archive), str(source_url.path)]
            elif suffixes[-1] == ".tar":
                command = "tar"
                args = ["cf", str(tmp_dst_archive), str(source_url.path)]
            elif suffixes[-1] == ".gz":
                command = "gzip"
                args = [str(tmp_dst_archive), str(source_url.path)]
            elif suffixes[-1] == ".zip":
                command = "zip"
                args = [str(tmp_dst_archive), str(source_url.path)]
            else:
                raise ValueError(f"Don't know how to extract file {file.name}")

            click.echo(f"Running {command} {' '.join(args)}")
            subprocess = await asyncio.create_subprocess_exec(command, *args)
            returncode = await subprocess.wait()
            if returncode != 0:
                raise click.ClickException(f"Extraction failed: {subprocess.stderr}")
            else:
                source_url = URL(str(tmp_dst_archive))
                # At this moment we know destination URL is a file, but we need its
                # parent directory
                destination_file = Path(destination_url.path)
                destination_dir = destination_file.parent
                destination_url = URL.build(
                    scheme=destination_url.scheme, path=str(destination_dir)
                )

        # handle upload/rclone
        await _nonstorage_cp(source_url, destination_url, remove_source=True)
        if compress:
            if tmp_dst_archive.exists():
                tmp_dst_archive.unlink()


async def _nonstorage_cp(
    source_url: URL, destination_url: URL, remove_source: bool = False
) -> None:
    if "s3" in (source_url.scheme, destination_url.scheme):
        command = "aws"
        args = ["s3", "cp", str(source_url), str(destination_url)]
        if source_url.path.endswith("/"):
            args.insert(2, "--recursive")
    elif "gs" in (source_url.scheme, destination_url.scheme):
        command = "gsutil"
        args = ["-m", "cp", "-r", str(source_url), str(destination_url)]
    elif source_url.scheme == "" and destination_url.scheme == "":
        command = "rclone"
        args = [
            "copy",  # TODO: investigate usage of 'sync' for potential speedup.
            "--checkers=16",  # https://rclone.org/docs/#checkers-n , default is 8
            "--transfers=8",  # https://rclone.org/docs/#transfers-n , default is 4.
            "--verbose=1",  # default is 0, set 2 for debug
            str(source_url),
            str(destination_url),
        ]
        if remove_source:
            pass
            # args.insert(2, "--remove-source-files")
    else:
        raise ValueError("Unknown cloud provider")
    click.echo(f"Running {command} {' '.join(args)}")
    subprocess = await asyncio.create_subprocess_exec(command, *args)
    returncode = await subprocess.wait()
    if returncode != 0:
        raise click.ClickException("Cloud copy failed")
    elif UrlType.get_type(source_url) == UrlType.LOCAL:
        source_path = Path(source_url.path)
        if source_path.is_dir():
            dir_util.remove_tree(str(source_path))
        else:
            if source_path.exists():
                source_path.unlink()
