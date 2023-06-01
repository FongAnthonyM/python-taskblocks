""" asyncqueue.py
Extends the multiprocessing Queue by adding async methods and interrupts for blocking methods.
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
from multiprocessing import get_context
from multiprocessing.queues import Queue
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from queue import Empty, Full
from time import perf_counter
from typing import Any

# Third-Party Packages #

# Local Packages #
from ..synchronize import Interrupt
from .asyncqueueinterface import AsyncQueueInterface


# Definitions #
# Classes #
class AsyncQueue(Queue, AsyncQueueInterface):
    """Extends the multiprocessing Queue by adding async methods and interrupts for blocking methods.

    Class Attributes:
        _ignore_attributes: The attributes to not pickle when pickling.

    Attributes:
        get_interrupt: An event which can be set to interrupt the get method blocking.
        put_interrupt: An event which can be set to interrupt the ptt method blocking.

    Args:
        maxsize: The maximum number items that can be in the queue.
        space_wait: Determines if this queue will wait for the queue space to enqueue an item.
        ctx: The context for the Python multiprocessing.
    """
    _ignore_attributes: set[str] = set(Queue(ctx=get_context()).__dict__.keys())

    # Magic Methods #
    # Construction/Destruction
    def __init__(self, maxsize: int = 0, space_wait: bool = False, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.get_interrupt: Interrupt = Interrupt()
        self.put_interrupt: Interrupt = Interrupt()

        self.space_wait: bool = space_wait

        # Construction #
        super().__init__(maxsize=maxsize, ctx=get_context() if ctx is None else ctx)

    # Pickling
    def __getstate__(self) -> Any:
        """Creates a dictionary of attributes which can be used to rebuild this object

        Returns:
            A dictionary of this object's attributes.
        """
        state = self.__dict__.copy()
        for name in self._ignore_attributes:
            del state[name]
        return Queue.__getstate__(self), state

    def __setstate__(self, state: Any) -> None:
        """Builds this object based on a dictionary of corresponding attributes.

        Args:
            state: The attributes to build this object from.
        """
        Queue.__setstate__(self, state[0])
        self.__dict__.update(state[1])

    # Instance Methods #
    # Queue
    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """Gets an item from the queue, waits for an item if the queue is empty.

        Args:
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for an item in the queue.

        Returns:
            The requested item.

        Raises:
            Empty: When there are no items to get in the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by the interrupt event.
        """
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        interrupted = self.get_interrupt.is_set()
        res = None

        # Try to get an object without blocking.
        if not block:
            if self._rlock.acquire(block=False):
                try:
                    if self._poll():
                        res = self._recv_bytes()
                        self._sem.release()
                finally:
                    self._rlock.release()

        # Try to get an object without timing out.
        elif timeout is None:
            while not (interrupted := self.get_interrupt.is_set()):  # Walrus operator sets and evaluates.
                if self._rlock.acquire(block=False):
                    try:
                        if self._poll():
                            res = self._recv_bytes()
                            self._sem.release()
                            break
                    finally:
                        self._rlock.release()

        # Try to get an object and timing out when specified.
        else:
            deadline = perf_counter() + timeout
            while not (interrupted := self.get_interrupt.is_set()):  # Walrus operator sets and evaluates.
                if self._rlock.acquire(block=False):
                    try:
                        if self._poll():
                            res = self._recv_bytes()
                            self._sem.release()
                            break
                    finally:
                        self._rlock.release()
                if deadline is not None and deadline <= perf_counter():
                    break

        # Determine what to do.
        if interrupted:
            raise InterruptedError
        elif res is not None:
            return ForkingPickler.loads(res)  # Unserialize the data after having released the lock
        else:
            raise Empty

    async def get_async(self, block: bool = True, timeout: float | None = None, interval: float = 0.0) -> Any:
        """Asynchronously gets an item from the queue, waits for an item if the queue is empty.

        Args:
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for an item in the queue.
            interval: The time, in seconds, between each queue check.

        Returns:
            The requested item.

        Raises:
            Empty: When there are no items to get in the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by the interrupt event.
        """
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        interrupted = self.get_interrupt.is_set()
        res = None

        # Try to get an object without blocking.
        if not block:
            if self._rlock.acquire(block=False):
                try:
                    if self._poll():
                        res = self._recv_bytes()
                        self._sem.release()
                finally:
                    self._rlock.release()

        # Try to get an object without timing out.
        elif timeout is None:
            while not (interrupted := self.get_interrupt.is_set()):  # Walrus operator sets and evaluates.
                if self._rlock.acquire(block=False):
                    try:
                        if self._poll():
                            res = self._recv_bytes()
                            self._sem.release()
                            break
                    finally:
                        self._rlock.release()
                    await sleep(interval)

        # Try to get an object and timing out when specified.
        else:
            deadline = perf_counter() + timeout
            while not (interrupted := self.get_interrupt.is_set()):  # Walrus operator sets and evaluates.
                if self._rlock.acquire(block=False):
                    try:
                        if self._poll():
                            res = self._recv_bytes()
                            self._sem.release()
                            break
                    finally:
                        self._rlock.release()
                if deadline is not None and deadline <= perf_counter():
                    break
                await sleep(interval)

        # Determine what to do.
        if interrupted:
            raise InterruptedError
        elif res is not None:
            return ForkingPickler.loads(res)  # Unserialize the data after having released the lock
        else:
            raise Empty

    def put(self, obj: Any, block: bool = True, timeout: float | None = None) -> None:
        """Puts an object into the queue, waits for access to the queue.

        Args:
            obj: The object to put into the queue.
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for space in the queue.

        Raises:
            Full: When there is no more space to put an item in the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by the interrupt event.
        """
        if self._closed:
            raise ValueError(f"Queue {self!r} is closed")

        # Try to put an object without blocking.
        if not block:
            super().put(obj=obj, block=block, timeout=timeout)
            return

        # Try to put an object without timing out.
        elif timeout is None:
            while not self.put_interrupt.is_set():
                if self._sem.acquire(block=False):
                    with self._notempty:
                        if self._thread is None:
                            self._start_thread()
                        self._buffer.append(obj)
                        self._notempty.notify()
                        return

        # Try to put an object and timing out when specified.
        else:
            deadline = perf_counter() + timeout
            while not self.put_interrupt.is_set():
                if self._sem.acquire(block=False):
                    with self._notempty:
                        if self._thread is None:
                            self._start_thread()
                        self._buffer.append(obj)
                        self._notempty.notify()
                        return
                if deadline is not None and deadline <= perf_counter():
                    raise Full

        # Interruption leads to an error.
        raise InterruptedError

    async def put_async(self, obj: Any, timeout: float | None = None, interval: float = 0.0) -> None:
        """Asynchronously puts an object into the queue, waits for access to the queue.

        Args:
            obj: The object to put into the queue.
            timeout: The time, in seconds, to wait for space in the queue.
            interval: The time, in seconds, between each access check.

        Raises:
            Full: When there is no more space to put an item in the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by the interrupt event.
        """
        if timeout is None:
            while not self.put_interrupt.is_set():
                if self._sem.acquire(block=False):
                    with self._notempty:
                        if self._thread is None:
                            self._start_thread()
                        self._buffer.append(obj)
                        self._notempty.notify()
                        return

                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while not self.put_interrupt.is_set():
                if self._sem.acquire(block=False):
                    with self._notempty:
                        if self._thread is None:
                            self._start_thread()
                        self._buffer.append(obj)
                        self._notempty.notify()
                        return
                if deadline is not None and deadline <= perf_counter():
                    raise Full

                await sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError
