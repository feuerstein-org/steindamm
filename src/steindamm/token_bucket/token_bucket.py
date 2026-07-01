"""
Public base classes for token bucket rate limiters.

`SyncTokenBucket` and `AsyncTokenBucket` are the base classes that both the local
(in-memory) and the Redis-backed buckets inherit from. Because the concrete
implementations are real subclasses, you can use these names as type annotations
and in ``isinstance`` checks:

.. code-block:: python

    def build() -> SyncTokenBucket:
        return SyncTokenBucket.create(name="api", capacity=10)

    assert isinstance(build(), SyncTokenBucket)

To construct a bucket, use the :meth:`create` classmethod. It selects the
implementation based on whether a Redis ``connection`` is provided:

.. code-block:: python

    bucket = SyncTokenBucket.create(name="api", capacity=10)              # local
    bucket = SyncTokenBucket.create(connection=redis, name="api")         # redis

You can also import and instantiate the concrete classes directly:
 - SyncLocalTokenBucket / AsyncLocalTokenBucket
 - SyncRedisTokenBucket / AsyncRedisTokenBucket
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, Self

from steindamm.token_bucket.token_bucket_base import TokenBucketBase

if TYPE_CHECKING:
    from datetime import datetime
    from types import TracebackType

    from redis import Redis as SyncRedis
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster


# Runtime availability check
try:
    import redis  # noqa: F401

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


_REDIS_IMPORT_ERROR = "Redis support requires the 'redis' package. Install it with: pip install steindamm[redis]"


# Defaults are defined here and in TokenBucketBase to help with typehints - keep them in sync
class SyncTokenBucket(TokenBucketBase):
    """
    Base class for synchronous token buckets.

    This is an abstract base: instantiate a concrete bucket with :meth:`create`
    (which selects a Redis or local backend based on ``connection``), or import
    ``SyncLocalTokenBucket`` / ``SyncRedisTokenBucket`` and construct them
    directly. The class defines the shared context-manager protocol; subclasses
    only implement :meth:`_acquire_slot`.

    Example:
        .. code-block:: python

            bucket = SyncTokenBucket.create(name="api", capacity=10)
            with bucket:
                make_api_call()

    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        """Guard against instantiating the abstract base directly; use create() instead."""
        if cls is SyncTokenBucket:
            raise TypeError(
                "SyncTokenBucket is an abstract base class - call SyncTokenBucket.create(...) to build a bucket, "
                "or instantiate SyncLocalTokenBucket / SyncRedisTokenBucket directly."
            )
        return super().__new__(cls)

    @classmethod
    def create(  # noqa: PLR0913
        cls,
        name: str,
        capacity: float = 5.0,
        refill_frequency: float = 1.0,
        initial_tokens: float | None = None,
        refill_amount: float = 1.0,
        max_sleep: float = 30.0,
        expiry: int = 60,
        tokens_to_consume: float = 1.0,
        window_start_time: datetime | None = None,
        connection: SyncRedis | SyncRedisCluster | None = None,
    ) -> SyncTokenBucket:
        """
        Create a token bucket, selecting the backend from ``connection``.

        - If ``connection`` is provided: returns a ``SyncRedisTokenBucket``.
        - If ``connection`` is None: returns a ``SyncLocalTokenBucket``.

        Args:
            name: Unique identifier for this token bucket.
            capacity: Maximum number of tokens the bucket can hold.
            refill_frequency: Time in seconds between token refills.
            initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
            refill_amount: Number of tokens added per refill.
            max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
            expiry: Key expiry time in seconds - currently not implemented for local buckets.
            tokens_to_consume: Number of tokens to consume per operation.
            window_start_time: Optional datetime in the past for window alignment.
            connection: Optional Redis connection (SyncRedis or SyncRedisCluster).

        """
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(_REDIS_IMPORT_ERROR)
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.token_bucket.redis_token_bucket import SyncRedisTokenBucket

            return SyncRedisTokenBucket(
                connection=connection,
                name=name,
                capacity=capacity,
                refill_frequency=refill_frequency,
                initial_tokens=initial_tokens,
                refill_amount=refill_amount,
                max_sleep=max_sleep,
                expiry=expiry,
                tokens_to_consume=tokens_to_consume,
                window_start_time=window_start_time,
            )
        from steindamm.token_bucket.local_token_bucket import SyncLocalTokenBucket

        return SyncLocalTokenBucket(
            name=name,
            capacity=capacity,
            refill_frequency=refill_frequency,
            initial_tokens=initial_tokens,
            refill_amount=refill_amount,
            max_sleep=max_sleep,
            expiry=expiry,
            tokens_to_consume=tokens_to_consume,
            window_start_time=window_start_time,
        )

    def __call__(self, tokens_to_consume: float | None = None) -> Self:
        """
        Bind a custom ``tokens_to_consume`` for the next context-manager entry.

        Args:
            tokens_to_consume: Number of tokens to consume. If None, uses the instance's
                tokens_to_consume value set during initialization.

        Example:
            .. code-block:: python

                bucket = SyncTokenBucket.create(name="api", capacity=10)
                # Consume 1 token (default)
                with bucket:
                    make_small_request()
                # Consume 5 tokens
                with bucket(5):
                    make_large_request()

        """
        self._temp_tokens_to_consume = tokens_to_consume
        return self

    def __enter__(self) -> None:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Use temporary value if set by __call__, otherwise use instance default
        tokens_needed = (
            self._temp_tokens_to_consume if self._temp_tokens_to_consume is not None else self.tokens_to_consume
        )

        if tokens_needed == 0:
            return

        # Clear temporary value
        self._temp_tokens_to_consume = None

        timestamp = self._acquire_slot(tokens_needed)

        sleep_time = self.parse_timestamp(timestamp)
        if sleep_time == 0:
            return

        time.sleep(sleep_time)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return

    def _acquire_slot(self, tokens_needed: float) -> float:
        """Run the backend token-bucket algorithm and return the slot timestamp in milliseconds."""
        raise NotImplementedError


# Defaults are defined here and in TokenBucketBase to help with typehints - keep them in sync
class AsyncTokenBucket(TokenBucketBase):
    """
    Base class for asynchronous token buckets.

    This is an abstract base: instantiate a concrete bucket with :meth:`create`
    (which selects a Redis or local backend based on ``connection``), or import
    ``AsyncLocalTokenBucket`` / ``AsyncRedisTokenBucket`` and construct them
    directly. The class defines the shared context-manager protocol; subclasses
    only implement :meth:`_acquire_slot`.

    Example:
        .. code-block:: python

            bucket = AsyncTokenBucket.create(name="api", capacity=10)
            async with bucket:
                await make_api_call()

    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        """Guard against instantiating the abstract base directly; use create() instead."""
        if cls is AsyncTokenBucket:
            raise TypeError(
                "AsyncTokenBucket is an abstract base class - call AsyncTokenBucket.create(...) to build a bucket, "
                "or instantiate AsyncLocalTokenBucket / AsyncRedisTokenBucket directly."
            )
        return super().__new__(cls)

    @classmethod
    def create(  # noqa: PLR0913
        cls,
        name: str,
        capacity: float = 5.0,
        refill_frequency: float = 1.0,
        initial_tokens: float | None = None,
        refill_amount: float = 1.0,
        max_sleep: float = 30.0,
        expiry: int = 60,
        tokens_to_consume: float = 1.0,
        window_start_time: datetime | None = None,
        connection: AsyncRedis | AsyncRedisCluster | None = None,
    ) -> AsyncTokenBucket:
        """
        Create a token bucket, selecting the backend from ``connection``.

        - If ``connection`` is provided: returns an ``AsyncRedisTokenBucket``.
        - If ``connection`` is None: returns an ``AsyncLocalTokenBucket``.

        Args:
            name: Unique identifier for this token bucket.
            capacity: Maximum number of tokens the bucket can hold.
            refill_frequency: Time in seconds between token refills.
            initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
            refill_amount: Number of tokens added per refill.
            max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
            expiry: Key expiry time in seconds - currently not implemented for local buckets.
            tokens_to_consume: Number of tokens to consume per operation.
            window_start_time: Optional datetime in the past for window alignment.
            connection: Optional async Redis connection (AsyncRedis or AsyncRedisCluster).

        """
        if connection is not None:
            if not REDIS_AVAILABLE:
                raise ImportError(_REDIS_IMPORT_ERROR)
            # Import only when needed to avoid requiring redis at module load time
            from steindamm.token_bucket.redis_token_bucket import AsyncRedisTokenBucket

            return AsyncRedisTokenBucket(
                connection=connection,
                name=name,
                capacity=capacity,
                refill_frequency=refill_frequency,
                initial_tokens=initial_tokens,
                refill_amount=refill_amount,
                max_sleep=max_sleep,
                expiry=expiry,
                tokens_to_consume=tokens_to_consume,
                window_start_time=window_start_time,
            )
        from steindamm.token_bucket.local_token_bucket import AsyncLocalTokenBucket

        return AsyncLocalTokenBucket(
            name=name,
            capacity=capacity,
            refill_frequency=refill_frequency,
            initial_tokens=initial_tokens,
            refill_amount=refill_amount,
            max_sleep=max_sleep,
            expiry=expiry,
            tokens_to_consume=tokens_to_consume,
            window_start_time=window_start_time,
        )

    def __call__(self, tokens_to_consume: float | None = None) -> Self:
        """
        Bind a custom ``tokens_to_consume`` for the next context-manager entry.

        Args:
            tokens_to_consume: Number of tokens to consume. If None, uses the instance's
                tokens_to_consume value set during initialization.

        Example:
            .. code-block:: python

                bucket = AsyncTokenBucket.create(name="api", capacity=10)
                # Consume 1 token (default)
                async with bucket:
                    await make_small_request()
                # Consume 5 tokens
                async with bucket(5):
                    await make_large_request()

        """
        self._temp_tokens_to_consume = tokens_to_consume
        return self

    async def __aenter__(self) -> None:
        """Acquire token(s) from the token bucket and sleep until they are available."""
        # Use temporary value if set by __call__, otherwise use instance default
        tokens_needed = (
            self._temp_tokens_to_consume if self._temp_tokens_to_consume is not None else self.tokens_to_consume
        )

        if tokens_needed == 0:
            return

        # Clear temporary value
        self._temp_tokens_to_consume = None

        timestamp = await self._acquire_slot(tokens_needed)

        sleep_time = self.parse_timestamp(timestamp)
        if sleep_time == 0:
            return

        await asyncio.sleep(sleep_time)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return

    async def _acquire_slot(self, tokens_needed: float) -> float:
        """Run the backend token-bucket algorithm and return the slot timestamp in milliseconds."""
        raise NotImplementedError
