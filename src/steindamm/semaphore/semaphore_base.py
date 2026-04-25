"""
Base class for semaphore limiters.

Defines common configuration parameters and shared helpers used by both local
and Redis-backed semaphore implementations.
"""

from typing import Annotated

from pydantic import BaseModel, Field

PositiveInt = Annotated[int, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]


class SemaphoreBase(BaseModel):
    """
    Base class shared by local and Redis-backed semaphore implementations.

    Args:
        name: Unique identifier for this semaphore.
        capacity: Maximum number of concurrent holders allowed.
        expiry: Key expiry time in seconds.
        max_sleep: Maximum seconds to wait for a slot. 0 means wait indefinitely.

    """

    name: str
    capacity: PositiveInt = 5
    expiry: PositiveInt = 60
    max_sleep: NonNegativeFloat = 30

    @property
    def key(self) -> str:
        """Key to use for the semaphore list."""
        return f"{{limiter}}:semaphore:{self.name}"

    @property
    def _exists_key(self) -> str:
        """Key to use when checking if the semaphore list has been created or not."""
        return f"{{limiter}}:semaphore:{self.name}-exists"

    def __str__(self) -> str:
        return f"Semaphore instance for queue {self.key}"
