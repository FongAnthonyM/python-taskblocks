""" simpleasyncqueue.py
Extends the multiprocessing SimpleQueue by adding async methods and interrupts for blocking methods.
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
from multiprocessing.queues import SimpleQueue
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from queue import Empty
from time import perf_counter
from typing import Any

# Third-Party Packages #

# Local Packages #
from .interrupt import Interrupt


# Definitions #
# Classes #
class SimpleAsyncQueue(SimpleQueue):
    """Extends the multiprocessing SimpleQueue by adding async methods and interrupts for blocking methods.

    Attributes:
        get_interrupt: An event which can be set to interrupt the get method blocking.
        put_interrupt: An event which can be set to interrupt the put method blocking.

    Args:
        ctx: The context for the Python multiprocessing.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.get_interrupt: Interrupt = Interrupt()
        self.put_interrupt: Interrupt = Interrupt()

        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    def __del__(self) -> None:
        """Closes the queue when this object is deleted."""
        self.close()

    # Instance Methods #
    def get(self, timeout: float | None = None) -> Any:
        """Gets an item from the queue, waits for an item if the queue is empty.

        Args:
            timeout: The time, in seconds, to wait for an item in the queue.

        Returns:
            The requested item.
        """
        res = None
        if timeout is None:
            while not self.get_interrupt.is_set():
                if self._rlock.acquire(block=False):
                    try:
                        if self._reader.poll():
                            res = self._reader.recv_bytes()
                            break
                    finally:
                        self._rlock.release()
        else:
            deadline = perf_counter() + timeout
            while not self.get_interrupt.is_set():
                if self._rlock.acquire(block=False):
                    try:
                        if self._reader.poll():
                            res = self._reader.recv_bytes()
                            break
                    finally:
                        self._rlock.release()
                elif deadline is not None and deadline <= perf_counter():
                    raise Empty

        if res is not None:
            # unserialize the data after having released the lock
            return ForkingPickler.loads(res)
        else:
            raise InterruptedError

    async def get_async(self, timeout: float | None = None, interval: float = 0.0) -> Any:
        """Asynchronously gets an item from the queue, waits for an item if the queue is empty.

        Args:
            timeout: The time, in seconds, to wait for an item in the queue.
            interval: The time, in seconds, between each queue check.

        Returns:
            The requested item.
        """
        res = None
        if timeout is None:
            while not self.get_interrupt.is_set():
                if self._rlock.acquire(block=False):
                    try:
                        if self._reader.poll():
                            res = self._reader.recv_bytes()
                            break
                    finally:
                        self._rlock.release()

                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while not self.get_interrupt.is_set():
                if self._rlock.acquire(block=False):
                    try:
                        if self._reader.poll():
                            res = self._reader.recv_bytes()
                            break
                    finally:
                        self._rlock.release()
                elif deadline is not None and deadline <= perf_counter():
                    raise Empty

                await sleep(interval)

        if res is not None:
            # unserialize the data after having released the lock
            return ForkingPickler.loads(res)
        else:
            raise InterruptedError

    def put_bytes(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        """Puts a bytes object into the queue, waits for access to the queue.

        Args:
            buf: The bytes buffer to put into the queue.
            offset: The offset in the buffer to put the bytes put into the queue.
            size: The amount of the bytes to put into the queue.
        """
        # serialize the data before acquiring the lock
        if self._wlock is None:
            # writes to a message oriented win32 pipe are atomic
            self._writer.send_bytes(buf, offset, size)
        else:
            with self._wlock:
                while not self.put_interrupt.is_set():
                    if self._wlock.acquire(block=False):
                        try:
                            self._writer.send_bytes(buf, offset, size)
                            return
                        finally:
                            self._wlock.release()

    async def put_bytes_async(
        self,
        buf: bytes,
        offset: int = 0,
        size: int | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> None:
        """Asynchronously puts a bytes object into the queue, waits for access to the queue.

        Args:
            buf: The bytes buffer to put into the queue.
            offset: The offset in the buffer to put the bytes put into the queue.
            size: The amount of the bytes to put into the queue.
            timeout: The time, in seconds, to wait for access to the queue.
            interval: The time, in seconds, between each access check.
        """
        # serialize the data before acquiring the lock
        if self._wlock is None:
            # writes to a message oriented win32 pipe are atomic
            self._writer.send_bytes(buf, offset, size)
        elif timeout is None:
            while not self.put_interrupt.is_set():
                if self._wlock.acquire(block=False):
                    try:
                        self._writer.send_bytes(buf, offset, size)
                        return
                    finally:
                        self._wlock.release()

                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while not self.put_interrupt.is_set():
                if self._wlock.acquire(block=False):
                    try:
                        self._writer.send_bytes(buf, offset, size)
                        return
                    finally:
                        self._wlock.release()
                if deadline <= perf_counter():
                    raise TimeoutError

                await sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError

    def put(self, obj: Any,) -> None:
        """Puts an object into the queue, waits for access to the queue.

        Args:
            obj: The object to put into the queue.
        """
        self.put_bytes(ForkingPickler.dumps(obj))

    async def put_async(self, obj: Any, timeout: float | None = None, interval: float = 0.0) -> None:
        """Asynchronously puts an object into the queue, waits for access to the queue.

        Args:
            obj: The object to put into the queue.
            timeout: The time, in seconds, to wait for access to the queue.
            interval: The time, in seconds, between each access check.
        """
        await self.put_bytes_async(ForkingPickler.dumps(obj), timeout=timeout, interval=interval)
