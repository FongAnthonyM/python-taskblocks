""" asynclock.py
Extends the multiprocessing Lock by adding async methods and interrupts for blocking methods.
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
from asyncio import sleep
from contextlib import contextmanager
from multiprocessing import get_context
from multiprocessing.synchronize import Lock
from multiprocessing.context import BaseContext
from time import perf_counter

# Third-Party Packages #

# Local Packages #
from .interrupts import Interrupt


# Definitions #
# Classes #
class AsyncLock(Lock):
    """Extends the multiprocessing Lock by adding async methods and interrupts for blocking methods.

    Attributes:
        acquire_interrupt: An event which can be set to interrupt the acquire method blocking.

    Args:
        ctx: The context for the Python multiprocessing.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.acquire_interrupt: Interrupt = Interrupt()

        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    # Instance Methods #
    def acquire(self, block: bool = True, timeout: float | None = None) -> bool:
        """Acquires the Lock, waits for the lock if block is True.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.

        Returns:
            If this method successful acquired or failed to a timeout.

        Raises:
            InterruptedError: When this method is interrupted by an interrupt event.
        """
        if not block:
            return super().acquire(block=block, timeout=timeout)
        elif timeout is None:
            while not self.acquire_interrupt.is_set():
                if super().acquire(False):
                    return True
        else:
            deadline = perf_counter() + timeout
            while not self.acquire_interrupt.is_set():
                if super().acquire(False):
                    return True
                if deadline <= perf_counter():
                    return False

        # Interruption leads to an error.
        raise InterruptedError

    async def acquire_async(self, block: bool = True, timeout: float | None = None, interval: float = 0.0) -> bool:
        """Asynchronously acquires the Lock, waits for the lock if block is True.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            If this method successful acquired or failed to a timeout.

        Raises:
            InterruptedError: When this method is interrupted by an interrupt event.
        """
        if not block:
            return super().acquire(block=block, timeout=timeout)
        elif timeout is None:
            while not self.acquire_interrupt.is_set():
                if super().acquire(False):
                    return True
                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while not self.acquire_interrupt.is_set():
                if super().acquire(False):
                    return True

                if deadline <= perf_counter():
                    return False

                await sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError

    @contextmanager
    async def async_context(self, block: bool = True, timeout: float | None = None, interval: float = 0.0) -> "Lock":
        """Asynchronous context manager for acquiring and releasing the Lock.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            This Lock object.
        """
        try:
            await self.acquire_async(block=block, timeout=timeout, interval=interval)
            yield self
        finally:
            self.release()
