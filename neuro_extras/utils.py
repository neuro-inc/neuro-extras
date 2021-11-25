import asyncio
import itertools
import logging
import os
import sys
import threading
import warnings
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Type

import neuro_sdk


logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_neuro_client(
    cluster: Optional[str] = None,
) -> AsyncIterator[neuro_sdk.Client]:
    client: neuro_sdk.Client = await neuro_sdk.get()
    cluster_orig: Optional[str] = None
    try:
        await client.__aenter__()

        cluster_orig = client.cluster_name
        if cluster is not None:
            if cluster != cluster_orig:
                logger.info(
                    f"Temporarily switching cluster: {cluster_orig} -> {cluster}"
                )
                await client.config.switch_cluster(cluster)  # typing: ignore
            else:
                logger.info(f"Already on cluster: {cluster}")
        yield client
    finally:
        # NOTE: bypass https://github.com/neuro-inc/platform-client-python/issues/1816
        try:
            await client.__aexit__(None, None, None)
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            logger.warning(f"Ignoring exception during closing neuro client: {e}")

        if cluster is not None and cluster_orig is not None and cluster != cluster_orig:
            logger.info(f"Switching back cluster: {cluster} -> {cluster_orig}")
            try:
                await client.config.switch_cluster(cluster_orig)
            except asyncio.CancelledError:
                raise
            except BaseException:
                logger.error(
                    f"Could not switch back to cluster '{cluster_orig}'. Please "
                    f"run manually: 'neuro config switch-cluster {cluster_orig}'"
                )


if sys.platform != "win32":
    from asyncio.unix_events import AbstractChildWatcher

    _Callback = Callable[..., None]

    class ThreadedChildWatcher(AbstractChildWatcher):
        # Backport from Python 3.8

        """Threaded child watcher implementation.

        The watcher uses a thread per process
        for waiting for the process finish.

        It doesn't require subscription on POSIX signal
        but a thread creation is not free.

        The watcher has O(1) complexity, its performance doesn't depend
        on amount of spawn processes.
        """

        def __init__(self) -> None:
            self._pid_counter = itertools.count(0)
            self._threads: Dict[int, threading.Thread] = {}

        def close(self) -> None:
            pass

        def __enter__(self) -> "ThreadedChildWatcher":
            return self

        def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
        ) -> None:
            pass

        def __del__(self, _warn: Any = warnings.warn) -> None:
            threads = [
                thread for thread in list(self._threads.values()) if thread.is_alive()
            ]
            if threads:
                _warn(
                    f"{self.__class__} has registered but not finished child processes",
                    ResourceWarning,
                    source=self,
                )

        def add_child_handler(self, pid: int, callback: _Callback, *args: Any) -> None:
            loop = asyncio.get_event_loop()
            thread = threading.Thread(
                target=self._do_waitpid,
                name=f"waitpid-{next(self._pid_counter)}",
                args=(loop, pid, callback, args),
                daemon=True,
            )
            self._threads[pid] = thread
            thread.start()

        def remove_child_handler(self, pid: int) -> bool:
            # asyncio never calls remove_child_handler() !!!
            # The method is no-op but is implemented because
            # abstract base classe requires it
            return True

        def attach_loop(self, loop: Optional[asyncio.AbstractEventLoop]) -> None:
            pass

        def _do_waitpid(
            self,
            loop: asyncio.AbstractEventLoop,
            expected_pid: int,
            callback: _Callback,
            args: List[Any],
        ) -> None:
            assert expected_pid > 0

            try:
                pid, status = os.waitpid(expected_pid, 0)
            except ChildProcessError:
                # The child process is already reaped
                # (may happen if waitpid() is called elsewhere).
                pid = expected_pid
                returncode = 255
                logger.warning(
                    "Unknown child process pid %d, will report returncode 255", pid
                )
            else:
                returncode = _compute_returncode(status)
                if loop.get_debug():
                    logger.debug(
                        "process %s exited with returncode %s", expected_pid, returncode
                    )

            if loop.is_closed():
                logger.warning("Loop %r that handles pid %r is closed", loop, pid)
            else:
                loop.call_soon_threadsafe(callback, pid, returncode, *args)

            self._threads.pop(expected_pid)

    def _compute_returncode(status: int) -> int:
        if os.WIFSIGNALED(status):
            # The child process died because of a signal.
            return -os.WTERMSIG(status)
        elif os.WIFEXITED(status):
            # The child process exited (e.g sys.exit()).
            return os.WEXITSTATUS(status)
        else:
            # The child exited, but we don't understand its status.
            # This shouldn't happen, but if it does, let's just
            # return that status; perhaps that helps debug it.
            return status


def setup_child_watcher() -> None:
    if sys.platform == "win32":
        WindowsProactorEventLoopPolicy = asyncio.WindowsProactorEventLoopPolicy

        asyncio.set_event_loop_policy(WindowsProactorEventLoopPolicy())
    else:
        if sys.version_info < (3, 8):
            asyncio.set_child_watcher(ThreadedChildWatcher())
