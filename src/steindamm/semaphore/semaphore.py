"""
Factory classes for creating semaphore instances.

Each class will use a semaphore running locally unless a "connection"
parameter is provided for the Redis server/cluster.

You can also use the respective semaphore classes directly.
 - SyncLocalSemaphore
 - AsyncLocalSemaphore
 - SyncRedisSemaphore
 - AsyncRedisSemaphore
"""

from typing import TYPE_CHECKING

from steindamm.semaphore.local_semaphore import AsyncLocalSemaphore, SyncLocalSemaphore

if TYPE_CHECKING:
    from redis import Redis as SyncRedis
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster

    from steindamm.semaphore.redis_semaphore import AsyncRedisSemaphore, SyncRedisSemaphore


try:
    import redis  # noqa: F401

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class SyncSemaphore:
    """
    Factory class for creating synchronous semaphore instances.

    Automatically selects the appropriate implementation:
    - If `connection` is provided: uses Redis-backed semaphore (SyncRedisSemaphore)
    - If `connection` is None: uses local in-memory semaphore (SyncLocalSemaphore)

    You can also import SyncRedisSemaphore or SyncLocalSemaphore directly.

    Args:
        name: Unique identifier for this semaphore.
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds - currently not implemented for local semaphores.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.
        connection: Optional Redis connection (SyncRedis or SyncRedisCluster).
            If provided, uses Redis-based implementation; otherwise uses local in-memory.

    Examples:
        Local in-memory semaphore (no Redis required):

        .. code-block:: python

            semaphore = SyncSemaphore(name="api", capacity=5)
            with semaphore:
                make_api_call()

        Redis-based semaphore:

        .. code-block:: python

            from redis import Redis  # or from redis.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            semaphore = SyncSemaphore(connection=redis_conn, name="api", capacity=5)
            with semaphore:
                make_api_call()

    """

    def __new__(  # noqa: D102
        cls,
        name: str,
        capacity: int = 5,
        expiry: int = 60,
        max_sleep: float = 30.0,
        connection: "SyncRedis | SyncRedisCluster | None" = None,
    ) -> "SyncRedisSemaphore | SyncLocalSemaphore":
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(
                    "Redis support requires the 'redis' package. Install it with: pip install steindamm[redis]"
                )
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.semaphore.redis_semaphore import SyncRedisSemaphore

            return SyncRedisSemaphore(
                connection=connection,
                name=name,
                capacity=capacity,
                expiry=expiry,
                max_sleep=max_sleep,
            )

        return SyncLocalSemaphore(
            name=name,
            capacity=capacity,
            expiry=expiry,
            max_sleep=max_sleep,
        )


class AsyncSemaphore:
    """
    Factory class for creating asynchronous semaphore instances.

    Automatically selects the appropriate implementation:
    - If `connection` is provided: uses Redis-backed semaphore (AsyncRedisSemaphore)
    - If `connection` is None: uses local in-memory semaphore (AsyncLocalSemaphore)

    For explicit control over the implementation, import and use
    AsyncRedisSemaphore or AsyncLocalSemaphore directly.

    Args:
        name: Unique identifier for this semaphore.
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds - currently not implemented for local semaphores.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.
        connection: Optional async Redis connection (AsyncRedis or AsyncRedisCluster).
            If provided, uses Redis-based implementation; otherwise uses local in-memory.

    Examples:
        Local in-memory async semaphore:

        .. code-block:: python

            semaphore = AsyncSemaphore(name="api", capacity=5)
            async with semaphore:
                await make_api_call()

        Redis-based async semaphore:

        .. code-block:: python

            from redis.asyncio import Redis
            redis_conn = Redis(host='localhost', port=6379)
            semaphore = AsyncSemaphore(connection=redis_conn, name="api", capacity=5)
            async with semaphore:
                await make_api_call()

    """

    def __new__(  # noqa: D102
        cls,
        name: str,
        capacity: int = 5,
        expiry: int = 60,
        max_sleep: float = 30.0,
        connection: "AsyncRedis | AsyncRedisCluster | None" = None,
    ) -> "AsyncRedisSemaphore | AsyncLocalSemaphore":
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(
                    "Redis support requires the 'redis' package. Install it with: pip install steindamm[redis]"
                )
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.semaphore.redis_semaphore import AsyncRedisSemaphore

            return AsyncRedisSemaphore(
                connection=connection,
                name=name,
                capacity=capacity,
                expiry=expiry,
                max_sleep=max_sleep,
            )

        return AsyncLocalSemaphore(
            name=name,
            capacity=capacity,
            expiry=expiry,
            max_sleep=max_sleep,
        )
