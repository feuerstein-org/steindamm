"""
Various token bucket and semaphore implementations using a Redis or local backend.

Use SyncTokenBucket or AsyncTokenBucket to automatically select between Redis-based
and local in-memory implementations based on whether a Redis connection is provided.

For explicit control over the implementation, import and use
the explicit Redis or local classes directly.
"""

from typing import Any

from steindamm.exceptions import MaxSleepExceededError, NoTokensAvailableError
from steindamm.semaphore.local_semaphore import AsyncLocalSemaphore, SyncLocalSemaphore
from steindamm.token_bucket.local_token_bucket import AsyncLocalTokenBucket, SyncLocalTokenBucket
from steindamm.token_bucket.token_bucket import AsyncTokenBucket, SyncTokenBucket


def __getattr__(name: str) -> Any:
    """Lazy import for Redis-based classes to avoid requiring redis when not needed."""
    if name == "AsyncRedisTokenBucket":
        from steindamm.token_bucket.redis_token_bucket import AsyncRedisTokenBucket

        return AsyncRedisTokenBucket
    if name == "SyncRedisTokenBucket":
        from steindamm.token_bucket.redis_token_bucket import SyncRedisTokenBucket

        return SyncRedisTokenBucket
    if name == "AsyncRedisSemaphore":
        from steindamm.semaphore.redis_semaphore import AsyncRedisSemaphore

        return AsyncRedisSemaphore
    if name == "SyncRedisSemaphore":
        from steindamm.semaphore.redis_semaphore import SyncRedisSemaphore

        return SyncRedisSemaphore
    if name == "AsyncSemaphore":
        from steindamm.semaphore import AsyncSemaphore

        return AsyncSemaphore
    if name == "SyncSemaphore":
        from steindamm.semaphore import SyncSemaphore

        return SyncSemaphore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Return list of available module attributes including lazy-loaded ones."""
    return list(__all__)


__all__ = (
    "AsyncLocalSemaphore",
    "AsyncLocalTokenBucket",
    "AsyncRedisSemaphore",  # type: ignore[reportUnsupportedDunderAll]
    "AsyncRedisTokenBucket",  # type: ignore[reportUnsupportedDunderAll]
    "AsyncSemaphore",  # type: ignore[reportUnsupportedDunderAll]
    "AsyncTokenBucket",
    "MaxSleepExceededError",
    "NoTokensAvailableError",
    "SyncLocalSemaphore",
    "SyncLocalTokenBucket",
    "SyncRedisSemaphore",  # type: ignore[reportUnsupportedDunderAll]
    "SyncRedisTokenBucket",  # type: ignore[reportUnsupportedDunderAll]
    "SyncSemaphore",  # type: ignore[reportUnsupportedDunderAll]
    "SyncTokenBucket",
)
