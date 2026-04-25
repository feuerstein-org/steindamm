"""Synchronous and asynchronous Redis-backed semaphore implementations."""

from datetime import datetime
from logging import getLogger
from types import TracebackType
from typing import ClassVar

from redis.asyncio.client import Pipeline
from redis.asyncio.cluster import ClusterPipeline

from steindamm.base import AsyncLuaScriptBase, SyncLuaScriptBase
from steindamm.exceptions import MaxSleepExceededError
from steindamm.semaphore.semaphore_base import SemaphoreBase

logger = getLogger(__name__)


class SyncRedisSemaphore(SemaphoreBase, SyncLuaScriptBase):
    """
    Synchronous Redis-backed semaphore.

    Args:
        name: Unique identifier for this semaphore.
        connection: Redis connection (SyncRedis or SyncRedisCluster).
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.

    Example:

        .. code-block:: python

            from redis import Redis  # or from redis.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            semaphore = SyncRedisSemaphore(connection=redis_conn, name="api", capacity=5)
            with semaphore:
                make_api_call()

    """

    script_name: ClassVar[str] = "semaphore/semaphore.lua"

    def __enter__(self) -> None:
        """Call the semaphore Lua script to create a semaphore, then call BLPOP to acquire it."""
        # Retrieve timestamp for when to wake up from Redis
        # To understand what exists does, check the Lua script
        if self.script(
            keys=[self.key, self._exists_key],
            args=[self.capacity],
        ):
            logger.info("Created new semaphore `%s` with capacity %s", self.name, self.capacity)
        else:
            logger.debug("Skipped creating semaphore, since one exists")

        start = datetime.now()

        self.connection.blpop([self.key], self.max_sleep)
        pipeline = self.connection.pipeline()
        pipeline.expire(self.key, self.expiry)
        pipeline.expire(self._exists_key, self.expiry)
        pipeline.execute()

        if 0.0 < self.max_sleep < (datetime.now() - start).total_seconds():
            raise MaxSleepExceededError("Max sleep exceeded waiting for Semaphore")

        logger.debug("Acquired semaphore %s", self.name)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pipeline = self.connection.pipeline()
        pipeline.lpush(self.key, 1)
        pipeline.expire(self.key, self.expiry)
        pipeline.expire(self._exists_key, self.expiry)
        pipeline.execute()

        logger.debug("Released semaphore %s", self.name)


class AsyncRedisSemaphore(SemaphoreBase, AsyncLuaScriptBase):
    """
    Asynchronous Redis-backed semaphore.

    Args:
        name: Unique identifier for this semaphore.
        connection: Redis connection (AsyncRedis or AsyncRedisCluster).
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.

    Example:

        .. code-block:: python

            from redis.asyncio import Redis  # or from redis.asyncio.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            semaphore = AsyncRedisSemaphore(connection=redis_conn, name="api", capacity=5)
            async with semaphore:
                await make_api_call()

    """

    script_name: ClassVar[str] = "semaphore/semaphore.lua"

    async def __aenter__(self) -> None:
        """Call the semaphore Lua script to create a semaphore, then call BLPOP to acquire it."""
        # Retrieve timestamp for when to wake up from Redis
        # To understand what exists does, check the Lua script
        if await self.script(
            keys=[self.key, self._exists_key],
            args=[self.capacity],
        ):
            logger.info("Created new semaphore `%s` with capacity %s", self.name, self.capacity)
        else:
            logger.debug("Skipped creating semaphore, since one exists")

        start = datetime.now()

        await self.connection.blpop([self.key], self.max_sleep)  # type: ignore[union-attr]
        pipeline: Pipeline | ClusterPipeline = self.connection.pipeline()
        pipeline.expire(self.key, self.expiry)  # type: ignore[union-attr]
        pipeline.expire(self._exists_key, self.expiry)  # type: ignore[union-attr]
        await pipeline.execute()

        if 0.0 < self.max_sleep < (datetime.now() - start).total_seconds():
            raise MaxSleepExceededError(f"Max sleep ({self.max_sleep}s) exceeded waiting for Semaphore")

        logger.debug("Acquired semaphore %s", self.name)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pipeline: Pipeline[str] | ClusterPipeline[str] = self.connection.pipeline()
        pipeline.lpush(self.key, 1)  # type: ignore[union-attr]
        pipeline.expire(self.key, self.expiry)  # type: ignore[union-attr]
        pipeline.expire(self._exists_key, self.expiry)  # type: ignore[union-attr]
        await pipeline.execute()

        logger.debug("Released semaphore %s", self.name)
