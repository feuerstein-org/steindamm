"""Semaphore package exports."""

from typing import Any

from steindamm.semaphore.local_semaphore import AsyncLocalSemaphore, SyncLocalSemaphore
from steindamm.semaphore.semaphore import REDIS_AVAILABLE, AsyncSemaphore, SyncSemaphore


def __getattr__(name: str) -> Any:
    """Lazy import Redis semaphore classes so local imports work without redis installed."""
    if name == "AsyncRedisSemaphore":
        from steindamm.semaphore.redis_semaphore import AsyncRedisSemaphore

        return AsyncRedisSemaphore
    if name == "SyncRedisSemaphore":
        from steindamm.semaphore.redis_semaphore import SyncRedisSemaphore

        return SyncRedisSemaphore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Return list of available package attributes including lazy-loaded ones."""
    return list(__all__)


__all__ = (
    "REDIS_AVAILABLE",
    "AsyncLocalSemaphore",
    "AsyncRedisSemaphore",  # type: ignore[reportUnsupportedDunderAll]
    "AsyncSemaphore",
    "SyncLocalSemaphore",
    "SyncRedisSemaphore",  # type: ignore[reportUnsupportedDunderAll]
    "SyncSemaphore",
)
