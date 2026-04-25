"""Test Redis Cluster and Standalone connections with all limiters."""

import asyncio
from typing import cast

import pytest
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster

from steindamm import AsyncRedisSemaphore, AsyncRedisTokenBucket, SyncRedisSemaphore, SyncRedisTokenBucket


@pytest.mark.parametrize(
    "klass,port,limiters",
    [
        (SyncRedis, 6378, [SyncRedisSemaphore, SyncRedisTokenBucket]),
        (SyncRedisCluster, 6380, [SyncRedisSemaphore, SyncRedisTokenBucket]),
        (AsyncRedis, 6378, [AsyncRedisSemaphore, AsyncRedisTokenBucket]),
        (AsyncRedisCluster, 6380, [AsyncRedisSemaphore, AsyncRedisTokenBucket]),
    ],
)
def test_redis_cluster(
    klass: SyncRedis | AsyncRedis,
    port: int,
    limiters: list[type[SyncRedisTokenBucket | AsyncRedisTokenBucket | SyncRedisSemaphore | AsyncRedisSemaphore]],
) -> None:
    """Test that Redis Cluster and Standalone connections work with all limiters."""
    connection = klass.from_url(f"redis://127.0.0.1:{port}")
    if hasattr(connection, "__aenter__"):
        # Async connection
        async def test_async() -> None:
            async_connection = cast(AsyncRedis, connection)
            async with async_connection:
                await async_connection.info()

        asyncio.run(test_async())
    else:
        # Sync connection
        connection.info()

    semaphore_limiter, token_bucket_limiter = limiters

    semaphore_limiter(
        name="test",
        capacity=99,
        max_sleep=99,
        expiry=99,
        connection=connection,  # type: ignore[arg-type]
    )
    token_bucket_limiter(
        name="test",
        capacity=99,
        max_sleep=99,
        expiry=99,
        refill_frequency=99,
        refill_amount=99,
        connection=connection,  # type: ignore[arg-type]
    )
