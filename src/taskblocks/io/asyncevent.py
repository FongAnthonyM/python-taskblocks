""" asyncevent.py
Extends the multiprocessing Event by adding async methods and interrupts for blocking methods.
"""
# Package Header #
from ..header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from asyncio import sleep
from multiprocessing import get_context
from multiprocessing.synchronize import Event
from multiprocessing.context import BaseContext
from time import perf_counter

# Third-Party Packages #

# Local Packages #


# Definitions #
# Classes #
class AsyncEvent(Event):
    """Extends the multiprocessing Event by adding async methods and interrupts for blocking methods.

    Attributes:
        wait_interrupt: An event which can be set to interrupt the wait method blocking.
        hold_interrupt: An event which can be set to interrupt the hold method blocking.

    Args:
        ctx: The context for the Python multiprocessing.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.wait_interrupt: Event = Event(ctx=get_context())
        self.hold_interrupt: Event = Event(ctx=get_context())

        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    # Type Conversion
    def __bool__(self) -> bool:
        """Returns a boolean based on the state of this event."""
        return self.is_set()

    # Instance Methods #
    def wait(self, timeout: float | None = None) -> bool:
        """Waits for the Event to be changed to set.

        Args:
            timeout: The time, in seconds, to wait for the Event to be set, otherwise returns False.

        Returns:
            If this method successful waited or failed to a timeout.
        """
        if timeout is None:
            while not self.hold_interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return True
        else:
            deadline = perf_counter() + timeout
            while not self.hold_interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return True
                if deadline <= perf_counter():
                    return False

        # Interruption leads to an error.
        raise InterruptedError

    async def wait_async(self, timeout: float | None = None, interval: float = 0.0) -> bool:
        """Asynchronously waits for the Event to be changed to set.

        Args:
            timeout: The time, in seconds, to wait for the Event to be set, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            If this method successful waited or failed to a timeout.
        """
        if timeout is None:
            while not self.wait_interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return True
                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while not self.wait_interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return True

                if deadline <= perf_counter():
                    return False

                await sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError

    def hold(self, timeout: float | None = None) -> bool:
        """Waits for the Event to be changed to cleared.

        Args:
            timeout: The time, in seconds, to wait for the Event to be cleared, otherwise returns False.

        Returns:
            If this method successful waited for cleared or failed to a timeout.
        """
        if timeout is None:
            while not self.hold_interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return False
                    else:
                        return True
        else:
            deadline = perf_counter() + timeout
            while not self.hold_interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                    else:
                        return True

                if deadline <= perf_counter():
                    return False

        # Interruption leads to an error.
        raise InterruptedError

    async def hold_async(self, timeout: float | None = None, interval: float = 0.0) -> bool:
        """Waits for the Event to be changed to cleared.

        Args:
            timeout: The time, in seconds, to wait for the Event to be cleared, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            If this method successful waited for cleared or failed to a timeout.
        """
        if timeout is None:
            while not self.hold_interrupt.is_set():
                with self._cond:
                    if not self._flag.acquire(False):
                        return True
                    else:
                        self._flag.release()

                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while not self.hold_interrupt.is_set():
                with self._cond:
                    if not self._flag.acquire(False):
                        return True
                    else:
                        self._flag.release()

                if deadline <= perf_counter():
                    raise TimeoutError

                await sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError
