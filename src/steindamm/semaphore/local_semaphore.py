"""
Synchronous and asynchronous local semaphore implementations.

These implementations provide in-memory concurrency limiting for single-process
applications. They do not require Redis and are suitable for local development,
tests, and applications where coordination across multiple processes or machines
is not needed.
"""

import asyncio
import time
from threading import BoundedSemaphore, Lock
from types import TracebackType
from typing import ClassVar

from steindamm.exceptions import MaxSleepExceededError
from steindamm.semaphore.semaphore_base import SemaphoreBase


class SyncLocalSemaphore(SemaphoreBase):
    """
    Synchronous local semaphore.

    Args:
        name: Unique identifier for this semaphore.
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds - currently not implemented for local semaphores.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.

    Example:
        .. code-block:: python

            semaphore = SyncLocalSemaphore(name="api", capacity=5)
            with semaphore:
                make_api_call()

    Note:
        `expiry` is accepted for API compatibility but not enforced for local semaphores.

    """

    _semaphores: ClassVar[dict[str, BoundedSemaphore]] = {}
    _main_lock: ClassVar[Lock] = Lock()

    def _get_semaphore(self) -> BoundedSemaphore:
        if self.key not in self._semaphores:
            with self._main_lock:
                if self.key not in self._semaphores:
                    self._semaphores[self.key] = BoundedSemaphore(self.capacity)
        return self._semaphores[self.key]

    def __enter__(self) -> None:
        """Acquire a local semaphore slot, blocking until one is available."""
        if self.max_sleep == 0:
            acquired = self._get_semaphore().acquire()
        else:
            acquired = self._get_semaphore().acquire(timeout=self.max_sleep)

        if not acquired:
            raise MaxSleepExceededError("Max sleep exceeded waiting for Semaphore")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._get_semaphore().release()


class AsyncLocalSemaphore(SemaphoreBase):
    """
    Asynchronous local semaphore.

    Args:
        name: Unique identifier for this semaphore.
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds - currently not implemented for local semaphores.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.

    Example:
        .. code-block:: python

            semaphore = AsyncLocalSemaphore(name="api", capacity=5)
            async with semaphore:
                await make_api_call()

    Note:
        `expiry` is accepted for API compatibility but not enforced for local semaphores.

    """

    _semaphores: ClassVar[dict[str, asyncio.Semaphore]] = {}

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self.key not in self._semaphores:
            self._semaphores[self.key] = asyncio.Semaphore(self.capacity)
        return self._semaphores[self.key]

    async def __aenter__(self) -> None:
        """Acquire a local semaphore slot, blocking until one is available."""
        semaphore = self._get_semaphore()
        if self.max_sleep == 0:
            await semaphore.acquire()
            return

        start = time.perf_counter()
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=self.max_sleep)
        except TimeoutError as exc:
            raise MaxSleepExceededError(f"Max sleep ({self.max_sleep}s) exceeded waiting for Semaphore") from exc

        if time.perf_counter() - start > self.max_sleep:
            semaphore.release()
            raise MaxSleepExceededError(f"Max sleep ({self.max_sleep}s) exceeded waiting for Semaphore")

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._get_semaphore().release()
