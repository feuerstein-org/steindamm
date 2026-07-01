"""Synchronous and Asynchronous Redis-backed (Standalone or Cluster) token bucket implementations."""

from typing import ClassVar, cast

from steindamm.base import AsyncLuaScriptBase, SyncLuaScriptBase
from steindamm.exceptions import NoTokensAvailableError
from steindamm.token_bucket.token_bucket import AsyncTokenBucket, SyncTokenBucket


class SyncRedisTokenBucket(SyncTokenBucket, SyncLuaScriptBase):
    """
    Synchronous Redis-backed (Standalone or Cluster) token bucket.

    Args:
        name: Unique identifier for this token bucket.
        connection: Redis connection (SyncRedis or SyncRedisCluster).
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds.
        tokens_to_consume: Number of tokens to consume per operation.

    Example:
        .. code-block:: python

            from redis import Redis  # or from redis.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            bucket = SyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
            with bucket:
                make_api_call()

    """

    script_name: ClassVar[str] = "token_bucket/token_bucket.lua"

    def _acquire_slot(self, tokens_needed: float) -> float:
        """Run the Lua token-bucket script and return the slot timestamp in milliseconds."""
        try:
            return cast(
                float,
                self.script(
                    keys=[self.key],
                    args=[
                        self.capacity,
                        self.refill_amount,
                        cast(float, self.initial_tokens),  # Set in config validator
                        self.refill_frequency,
                        self.expiry,
                        tokens_needed,
                        self.max_sleep,
                        self._window_start_timestamp or 0,
                    ],
                ),
            )
        except Exception as e:
            error_msg = str(e)
            # Lua script will return exception if max_sleep is exceeded
            if "Time till next token exceeds max_sleep time:" in error_msg:
                sleep_time_str = error_msg.split(":")[-1].strip()
                sleep_time = float(sleep_time_str)
                self.raise_max_sleep_exception(sleep_time)  # Will raise MaxSleepExceededError
            # Lua script will return exception if non-refilling bucket runs out of tokens
            elif "No tokens available" in error_msg and "non-refilling bucket" in error_msg:
                # Parse available and requested tokens from error message
                try:
                    parts = error_msg.split(", ")
                    available = float(parts[0].split("Available: ")[1])
                    requested = float(parts[1].split("Requested: ")[1])
                except (IndexError, ValueError):
                    available = 0.0
                    requested = tokens_needed
                raise NoTokensAvailableError(
                    f"Token bucket '{self.name}' has run out of tokens. "
                    f"Available: {available}, Requested: {requested}. "
                    f"This is a non-refilling bucket (refill_amount={self.refill_amount}, "
                    f"refill_frequency={self.refill_frequency})."
                ) from None
            raise


class AsyncRedisTokenBucket(AsyncTokenBucket, AsyncLuaScriptBase):
    """
    Asynchronous Redis-backed (Standalone or Cluster) token bucket.

    Args:
        name: Unique identifier for this token bucket.
        connection: Redis connection (AsyncRedis or AsyncRedisCluster).
        capacity: Maximum number of tokens the bucket can hold.
        refill_frequency: Time in seconds between token refills.
        initial_tokens: Starting number of tokens. Defaults to capacity if not specified.
        refill_amount: Number of tokens added per refill.
        max_sleep: Maximum seconds to sleep when rate limited. 0 means no limit.
        expiry: Key expiry time in seconds.
        tokens_to_consume: Number of tokens to consume per operation.

    Example:
        .. code-block:: python

            from redis.asyncio import Redis  # or from redis.asyncio.cluster import RedisCluster
            redis_conn = Redis(host='localhost', port=6379)
            bucket = AsyncRedisTokenBucket(connection=redis_conn, name="api", capacity=10)
            async with bucket:
                await make_api_call()

    """

    script_name: ClassVar[str] = "token_bucket/token_bucket.lua"

    async def _acquire_slot(self, tokens_needed: float) -> float:
        """Run the Lua token-bucket script and return the slot timestamp in milliseconds."""
        try:
            return cast(
                float,
                await self.script(
                    keys=[self.key],
                    args=[
                        self.capacity,
                        self.refill_amount,
                        cast(float, self.initial_tokens),  # Set in config validator
                        self.refill_frequency,
                        self.expiry,
                        tokens_needed,
                        self.max_sleep,
                        self._window_start_timestamp or 0,
                    ],
                ),
            )
        except Exception as e:
            error_msg = str(e)
            # Lua script will return exception if max_sleep is exceeded
            if "Time till next token exceeds max_sleep time:" in error_msg:
                sleep_time_str = error_msg.split(":")[-1].strip()
                sleep_time = float(sleep_time_str)
                self.raise_max_sleep_exception(sleep_time)  # Will raise MaxSleepExceededError
            # Lua script will return exception if non-refilling bucket runs out of tokens
            elif "No tokens available" in error_msg and "non-refilling bucket" in error_msg:
                # Parse available and requested tokens from error message
                try:
                    parts = error_msg.split(", ")
                    available = float(parts[0].split("Available: ")[1])
                    requested = float(parts[1].split("Requested: ")[1])
                except (IndexError, ValueError):
                    available = 0.0
                    requested = tokens_needed
                raise NoTokensAvailableError(
                    f"Token bucket '{self.name}' has run out of tokens. "
                    f"Available: {available}, Requested: {requested}. "
                    f"This is a non-refilling bucket (refill_amount={self.refill_amount}, "
                    f"refill_frequency={self.refill_frequency})."
                ) from None
            raise
