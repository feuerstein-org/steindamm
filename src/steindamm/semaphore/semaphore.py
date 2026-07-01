"""
Public base classes for semaphore limiters.

`SyncSemaphore` and `AsyncSemaphore` are the base classes that both the local
(in-memory) and the Redis-backed semaphores inherit from. Because the concrete
implementations are real subclasses, you can use these names as type annotations
and in ``isinstance`` checks:

.. code-block:: python

    def build() -> SyncSemaphore:
        return SyncSemaphore.create(name="api", capacity=5)

    assert isinstance(build(), SyncSemaphore)

To construct a semaphore, use the :meth:`create` classmethod. It selects the
implementation based on whether a Redis ``connection`` is provided:

.. code-block:: python

    semaphore = SyncSemaphore.create(name="api", capacity=5)            # local
    semaphore = SyncSemaphore.create(connection=redis, name="api")      # redis

You can also import and instantiate the concrete classes directly:
 - SyncLocalSemaphore / AsyncLocalSemaphore
 - SyncRedisSemaphore / AsyncRedisSemaphore
"""

from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from steindamm.semaphore.semaphore_base import SemaphoreBase

if TYPE_CHECKING:
    from redis import Redis as SyncRedis
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster


try:
    import redis  # noqa: F401

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


_REDIS_IMPORT_ERROR = "Redis support requires the 'redis' package. Install it with: pip install steindamm[redis]"


# Defaults are defined here and in SemaphoreBase to help with typehints - keep them in sync
class SyncSemaphore(SemaphoreBase):
    """
    Base class for synchronous semaphores.

    This is an abstract base: instantiate a concrete semaphore with :meth:`create`
    (which selects a Redis or local backend based on ``connection``), or import
    ``SyncLocalSemaphore`` / ``SyncRedisSemaphore`` and construct them directly.
    Subclasses implement the context-manager protocol.

    Example:
        .. code-block:: python

            semaphore = SyncSemaphore.create(name="api", capacity=5)
            with semaphore:
                make_api_call()

    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        """Guard against instantiating the abstract base directly; use create() instead."""
        if cls is SyncSemaphore:
            raise TypeError(
                "SyncSemaphore is an abstract base class - call SyncSemaphore.create(...) to build a semaphore, "
                "or instantiate SyncLocalSemaphore / SyncRedisSemaphore directly."
            )
        return super().__new__(cls)

    @classmethod
    def create(
        cls,
        name: str,
        capacity: int = 5,
        expiry: int = 60,
        max_sleep: float = 30.0,
        connection: "SyncRedis | SyncRedisCluster | None" = None,
    ) -> "SyncSemaphore":
        """
        Create a semaphore, selecting the backend from ``connection``.

        - If ``connection`` is provided: returns a ``SyncRedisSemaphore``.
        - If ``connection`` is None: returns a ``SyncLocalSemaphore``.

        Args:
            name: Unique identifier for this semaphore.
            capacity: Maximum number of concurrent holders allowed.
            expiry: Key expiry time in seconds - currently not implemented for local semaphores.
            max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.
            connection: Optional Redis connection (SyncRedis or SyncRedisCluster).

        """
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(_REDIS_IMPORT_ERROR)
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.semaphore.redis_semaphore import SyncRedisSemaphore

            return SyncRedisSemaphore(
                connection=connection,
                name=name,
                capacity=capacity,
                expiry=expiry,
                max_sleep=max_sleep,
            )
        from steindamm.semaphore.local_semaphore import SyncLocalSemaphore

        return SyncLocalSemaphore(
            name=name,
            capacity=capacity,
            expiry=expiry,
            max_sleep=max_sleep,
        )

    def __enter__(self) -> None:
        """Acquire a semaphore slot, blocking until one is available."""
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        raise NotImplementedError


# Defaults are defined here and in SemaphoreBase to help with typehints - keep them in sync
class AsyncSemaphore(SemaphoreBase):
    """
    Base class for asynchronous semaphores.

    This is an abstract base: instantiate a concrete semaphore with :meth:`create`
    (which selects a Redis or local backend based on ``connection``), or import
    ``AsyncLocalSemaphore`` / ``AsyncRedisSemaphore`` and construct them directly.
    Subclasses implement the context-manager protocol.

    Example:
        .. code-block:: python

            semaphore = AsyncSemaphore.create(name="api", capacity=5)
            async with semaphore:
                await make_api_call()

    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        """Guard against instantiating the abstract base directly; use create() instead."""
        if cls is AsyncSemaphore:
            raise TypeError(
                "AsyncSemaphore is an abstract base class - call AsyncSemaphore.create(...) to build a semaphore, "
                "or instantiate AsyncLocalSemaphore / AsyncRedisSemaphore directly."
            )
        return super().__new__(cls)

    @classmethod
    def create(
        cls,
        name: str,
        capacity: int = 5,
        expiry: int = 60,
        max_sleep: float = 30.0,
        connection: "AsyncRedis | AsyncRedisCluster | None" = None,
    ) -> "AsyncSemaphore":
        """
        Create a semaphore, selecting the backend from ``connection``.

        - If ``connection`` is provided: returns an ``AsyncRedisSemaphore``.
        - If ``connection`` is None: returns an ``AsyncLocalSemaphore``.

        Args:
            name: Unique identifier for this semaphore.
            capacity: Maximum number of concurrent holders allowed.
            expiry: Key expiry time in seconds - currently not implemented for local semaphores.
            max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.
            connection: Optional async Redis connection (AsyncRedis or AsyncRedisCluster).

        """
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(_REDIS_IMPORT_ERROR)
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.semaphore.redis_semaphore import AsyncRedisSemaphore

            return AsyncRedisSemaphore(
                connection=connection,
                name=name,
                capacity=capacity,
                expiry=expiry,
                max_sleep=max_sleep,
            )
        from steindamm.semaphore.local_semaphore import AsyncLocalSemaphore

        return AsyncLocalSemaphore(
            name=name,
            capacity=capacity,
            expiry=expiry,
            max_sleep=max_sleep,
        )

    async def __aenter__(self) -> None:
        """Acquire a semaphore slot, blocking until one is available."""
        raise NotImplementedError

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        raise NotImplementedError
