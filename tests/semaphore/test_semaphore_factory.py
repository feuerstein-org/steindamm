"""Tests for SyncSemaphore and AsyncSemaphore factory classes."""

import pytest

from steindamm import (
    AsyncLocalSemaphore,
    AsyncRedisSemaphore,
    AsyncSemaphore,
    SyncLocalSemaphore,
    SyncRedisSemaphore,
    SyncSemaphore,
)
from tests.conftest import (
    CLUSTER_ASYNC_CONNECTION,
    CLUSTER_SYNC_CONNECTION,
    STANDALONE_ASYNC_CONNECTION,
    STANDALONE_SYNC_CONNECTION,
)


class TestSyncSemaphoreFactory:
    """Test that SyncSemaphore factory returns the correct class based on connection parameter."""

    def test_returns_local_semaphore_when_no_connection(self) -> None:
        """Test that SyncSemaphore returns SyncLocalSemaphore when connection is None."""
        semaphore = SyncSemaphore(name="test", capacity=2)
        assert isinstance(semaphore, SyncLocalSemaphore)
        assert not isinstance(semaphore, SyncRedisSemaphore)

    def test_returns_redis_semaphore_with_standalone_connection(self) -> None:
        """Test that SyncSemaphore returns SyncRedisSemaphore with Redis connection."""
        semaphore = SyncSemaphore(connection=STANDALONE_SYNC_CONNECTION(), name="test", capacity=2)
        assert isinstance(semaphore, SyncRedisSemaphore)
        assert not isinstance(semaphore, SyncLocalSemaphore)

    def test_returns_redis_semaphore_with_cluster_connection(self) -> None:
        """Test that SyncSemaphore returns SyncRedisSemaphore with cluster connection."""
        semaphore = SyncSemaphore(connection=CLUSTER_SYNC_CONNECTION(), name="test", capacity=2)
        assert isinstance(semaphore, SyncRedisSemaphore)
        assert not isinstance(semaphore, SyncLocalSemaphore)

    def test_passes_args_to_local_semaphore(self) -> None:
        """Test that all kwargs are passed correctly to SyncLocalSemaphore."""
        semaphore = SyncSemaphore(name="test", capacity=3, expiry=10, max_sleep=1.5)
        assert isinstance(semaphore, SyncLocalSemaphore)
        assert semaphore.name == "test"
        assert semaphore.capacity == 3
        assert semaphore.expiry == 10
        assert semaphore.max_sleep == 1.5

    def test_passes_args_to_redis_semaphore(self) -> None:
        """Test that all kwargs are passed correctly to SyncRedisSemaphore."""
        semaphore = SyncSemaphore(
            connection=STANDALONE_SYNC_CONNECTION(),
            name="test",
            capacity=3,
            expiry=10,
            max_sleep=1.5,
        )
        assert isinstance(semaphore, SyncRedisSemaphore)
        assert semaphore.name == "test"
        assert semaphore.capacity == 3
        assert semaphore.expiry == 10
        assert semaphore.max_sleep == 1.5

    def test_raises_import_error_when_redis_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that ImportError is raised when redis package is not available."""
        import steindamm.semaphore.semaphore as semaphore_module

        monkeypatch.setattr(semaphore_module, "REDIS_AVAILABLE", False)

        with pytest.raises(
            ImportError,
            match=r"Redis support requires the 'redis' package\. Install it with: pip install steindamm\[redis\]",
        ):
            SyncSemaphore(connection=STANDALONE_SYNC_CONNECTION(), name="test", capacity=2)


class TestAsyncSemaphoreFactory:
    """Test that AsyncSemaphore returns the correct implementation."""

    def test_returns_local_semaphore_when_no_connection(self) -> None:
        """Test that AsyncSemaphore returns AsyncLocalSemaphore when connection is None."""
        semaphore = AsyncSemaphore(name="test", capacity=2)
        assert isinstance(semaphore, AsyncLocalSemaphore)
        assert not isinstance(semaphore, AsyncRedisSemaphore)

    def test_returns_redis_semaphore_with_standalone_connection(self) -> None:
        """Test that AsyncSemaphore returns AsyncRedisSemaphore with Redis connection."""
        semaphore = AsyncSemaphore(connection=STANDALONE_ASYNC_CONNECTION(), name="test", capacity=2)
        assert isinstance(semaphore, AsyncRedisSemaphore)
        assert not isinstance(semaphore, AsyncLocalSemaphore)

    def test_returns_redis_semaphore_with_cluster_connection(self) -> None:
        """Test that AsyncSemaphore returns AsyncRedisSemaphore with cluster connection."""
        semaphore = AsyncSemaphore(connection=CLUSTER_ASYNC_CONNECTION(), name="test", capacity=2)
        assert isinstance(semaphore, AsyncRedisSemaphore)
        assert not isinstance(semaphore, AsyncLocalSemaphore)

    def test_passes_args_to_local_semaphore(self) -> None:
        """Test that all kwargs are passed correctly to AsyncLocalSemaphore."""
        semaphore = AsyncSemaphore(name="test", capacity=3, expiry=10, max_sleep=1.5)
        assert isinstance(semaphore, AsyncLocalSemaphore)
        assert semaphore.name == "test"
        assert semaphore.capacity == 3
        assert semaphore.expiry == 10
        assert semaphore.max_sleep == 1.5

    def test_passes_args_to_redis_semaphore(self) -> None:
        """Test that all kwargs are passed correctly to AsyncRedisSemaphore."""
        semaphore = AsyncSemaphore(
            connection=STANDALONE_ASYNC_CONNECTION(),
            name="test",
            capacity=3,
            expiry=10,
            max_sleep=1.5,
        )
        assert isinstance(semaphore, AsyncRedisSemaphore)
        assert semaphore.name == "test"
        assert semaphore.capacity == 3
        assert semaphore.expiry == 10
        assert semaphore.max_sleep == 1.5

    def test_raises_import_error_when_redis_not_available(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that ImportError is raised when redis package is not available."""
        import steindamm.semaphore.semaphore as semaphore_module

        monkeypatch.setattr(semaphore_module, "REDIS_AVAILABLE", False)

        with pytest.raises(
            ImportError,
            match=r"Redis support requires the 'redis' package\. Install it with: pip install steindamm\[redis\]",
        ):
            AsyncSemaphore(connection=STANDALONE_ASYNC_CONNECTION(), name="test", capacity=2)
