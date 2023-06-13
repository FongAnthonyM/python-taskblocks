""" lockedsharedmemory.py
SharedMemory with a Lock to ensure it is thread and process safe.
"""
# Package Header #
from ...header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from contextlib import contextmanager

# Third-Party Packages #

# Local Packages #
from ..synchronize import AsyncLock
from .sharedmemory import SharedMemory


# Definitions #
# Classes #
class LockedSharedMemory(SharedMemory):
    """SharedMemory with a Lock to ensure it is thread and process safe.

    Attributes:
        lock: The lock object for this SharedMemory

    Args:
        name: The name of the SharedMemory.
        create: Determines if the SharedMemory will be created if it does not exist.
        size: The number of bytes for this SharedMemory to allocate.
        register: Determines if the SharedMemory will be registered to help deallocate when the process dies it.
    """

    # Magic Methods #
    # Construction/Destruction
    def __init__(self, name: str | None = None, create: bool = False, size: int = 0, register: bool = True) -> None:
        # New Attributes #
        self.lock: AsyncLock = AsyncLock()

        # Parent Attributes #
        super().__init__(name=name, create=create, size=size, register=register)

    # Context Managers
    def __enter__(self) -> "LockedSharedMemory":
        """The context enter which acquires the lock.

        Returns:
            This object.
        """
        self.lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """The context exit which releases the lock."""
        self.lock.release()

    # Instance Methods #
    # Lock
    def as_unlocked(self, register: bool = True) -> SharedMemory:
        """Returns a SharedMemory with no lock.

        Args:
            register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.

        Returns:
            The same SharedMemory without a lock.
        """
        return SharedMemory(name=self.name, register=register)

    def acquire(self, block: bool = True, timeout: float | None = None) -> bool:
        """Acquires the Lock, waits for the lock if block is True.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.

        Returns:
            If this method successful acquired or failed to a timeout.
        """
        return self.lock.acquire(block=block, timeout=timeout)

    async def acquire_async(self, block: bool = True, timeout: float | None = None, interval: float = 0.0) -> bool:
        """Asynchronously acquires the Lock, waits for the lock if block is True.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            If this method successful acquired or failed to a timeout.
        """
        return await self.lock.acquire_async(block=block, timeout=timeout, interval=interval)

    def release(self) -> None:
        """Releases the lock."""
        self.lock.release()

    @contextmanager
    async def async_context(
        self,
        block: bool = True,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> "LockedSharedMemory":
        """Asynchronous context manager for acquiring and releasing the Lock.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            This object.
        """
        try:
            await self.lock.acquire_async(block=block, timeout=timeout, interval=interval)
            yield self
        finally:
            self.lock.release()
