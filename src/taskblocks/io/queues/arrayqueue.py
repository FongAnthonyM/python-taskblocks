""" arrayqueue.py

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
from asyncio import sleep, queues
from collections.abc import Iterable
from multiprocessing import Value
from multiprocessing.context import BaseContext
from queue import Full
from time import perf_counter
from typing import Any, NamedTuple

# Third-Party Packages #
from baseobjects import singlekwargdispatch
import numpy as np

# Local Packages #
from ..synchronize import Interrupt
from ..sharedmemory import SharedArray
from .asyncqueue import AsyncQueue


# Definitions #
# Classes #
class ArrayQueueItem(NamedTuple):
    array: SharedArray
    copy: bool
    delete: bool = True


class ArrayQueue(AsyncQueue):
    """

    Class Attributes:

    Attributes:

    Args:

    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        maxsize: int = 0,
        maxbytes: int = 10 ** 10,  # 10 Gigabytes
        bytes_wait: bool = False,
        *,
        ctx: BaseContext | None = None,
    ) -> None:
        # New Attributes #
        self.add_interrupt: Interrupt = Interrupt()
        self._maxbytes: Value = Value("q")
        self._n_bytes: Value = Value("q")

        self.bytes_wait: bool = bytes_wait

        # Construction #
        super().__init__(maxsize=maxsize, ctx=ctx)
        self.maxbytes = maxbytes
        with self._n_bytes:
            self._n_bytes.value = 0

    @property
    def maxbytes(self) -> int:
        with self._maxbytes:
            return self._maxbytes.value

    @maxbytes.setter
    def maxbytes(self, value: int) -> None:
        with self._maxbytes:
            self._maxbytes.value = value

    @property
    def n_bytes(self) -> int:
        with self._n_bytes:
            return self._n_bytes.value

    # Instance Methods #
    def _n_bytes_add(self, i: int):
        with self._n_bytes:
            self._n_bytes.value += i

    def space_check(self, size) -> bool:
        return size + self.n_bytes <= self.maxbytes

    def _add_bytes(self, size: int, block: bool = True, timeout: float | None = None) -> None:
        # Try to add bytes without blocking.
        if not block:
            if self._n_bytes.acquire(block=False):
                try:
                    if size + self._n_bytes.value <= self.maxbytes:
                        self._n_bytes.value += size
                        return
                    else:
                        raise Full
                finally:
                    self._n_bytes.release()

        # Try to add bytes without timing out.
        elif timeout is None:
            while not self.add_interrupt.is_set():
                if self._n_bytes.acquire(block=False):
                    try:
                        if size + self._n_bytes.value <= self.maxbytes:
                            self._n_bytes.value += size
                            return
                    finally:
                        self._n_bytes.release()

        # Try to add bytes and timing out when specified.
        else:
            deadline = perf_counter() + timeout
            while not self.add_interrupt.is_set():
                if self._n_bytes.acquire(block=False):
                    try:
                        if size + self._n_bytes.value <= self.maxbytes:
                            self._n_bytes.value += size
                            return
                    finally:
                        self._n_bytes.release()
                if deadline is not None and deadline <= perf_counter():
                    raise Full

        raise InterruptedError
    
    async def _add_bytes_async(
        self, 
        size: int, 
        block: bool = True, 
        timeout: float | None = None,  
        interval: float = 0.0,
    ) -> None:
        # Try to add bytes without blocking.
        if not block:
            if self._n_bytes.acquire(block=False):
                try:
                    if size + self._n_bytes.value <= self.maxbytes:
                        self._n_bytes.value += size
                        return
                    else:
                        raise Full
                finally:
                    self._n_bytes.release()

        # Try to add bytes without timing out.
        elif timeout is None:
            while not self.add_interrupt.is_set():
                if self._n_bytes.acquire(block=False):
                    try:
                        if size + self._n_bytes.value <= self.maxbytes:
                            self._n_bytes.value += size
                            return
                    finally:
                        self._n_bytes.release()
                await sleep(interval)

        # Try to add bytes and timing out when specified.
        else:
            deadline = perf_counter() + timeout
            while not self.add_interrupt.is_set():
                if self._n_bytes.acquire(block=False):
                    try:
                        if size + self._n_bytes.value <= self.maxbytes:
                            self._n_bytes.value += size
                            return
                    finally:
                        self._n_bytes.release()
                if deadline is not None and deadline <= perf_counter():
                    raise Full
                await sleep(interval)

        raise InterruptedError

    # Serialization
    @singlekwargdispatch(kwarg="obj")
    def serialize(self, obj: Any) -> Any:
        return obj

    @serialize.register(np.ndarray)
    def _serialize(self, obj: np.ndarray) -> ArrayQueueItem:
        a = SharedArray(a=obj)
        self._add_bytes(a._shared_memory.size, block=self.bytes_wait)
        a.close()
        return ArrayQueueItem(a, copy=True)

    @serialize.register(SharedArray)
    def _serialize(self, obj: SharedArray) -> ArrayQueueItem:
        self._add_bytes(obj._shared_memory.size, block=self.bytes_wait)
        obj.close()
        return ArrayQueueItem(obj, copy=False)

    @serialize.register(ArrayQueueItem)
    def _serialize(self, obj: ArrayQueueItem) -> ArrayQueueItem:
        self._add_bytes(obj.array._shared_memory.size, block=self.bytes_wait)
        obj.array.close()
        return obj

    @serialize.register(tuple)
    def _serialize(self, obj: ArrayQueueItem) -> tuple:
        return tuple(self.serialize(item) for item in obj)

    # Serialization Async
    @singlekwargdispatch(kwarg="obj")
    async def serialize_async(self, obj: Any) -> Any:
        return obj

    @serialize_async.register(np.ndarray)
    async def _serialize_async(self, obj: np.ndarray) -> ArrayQueueItem:
        a = SharedArray(a=obj)
        await self._add_bytes_async(a._shared_memory.size, block=self.bytes_wait)
        a.close()
        return ArrayQueueItem(a, copy=True)

    @serialize_async.register(SharedArray)
    async def _serialize_async(self, obj: SharedArray) -> ArrayQueueItem:
        await self._add_bytes_async(obj._shared_memory.size, block=self.bytes_wait)
        obj.close()
        return ArrayQueueItem(obj, copy=False)

    @serialize_async.register(ArrayQueueItem)
    async def _serialize_async(self, obj: ArrayQueueItem) -> ArrayQueueItem:
        await self._add_bytes_async(obj.array._shared_memory.size, block=self.bytes_wait)
        obj.array.close()
        return obj

    @serialize_async.register(tuple)
    async def _serialize_async(self, obj: ArrayQueueItem) -> tuple:
        return tuple(await self.serialize_async(item) for item in obj)

    # Deserialization
    @singlekwargdispatch("obj")
    def deserialize(self, obj: Any) -> Any:
        return obj

    @deserialize.register(ArrayQueueItem)
    def _deserialize(self, obj: ArrayQueueItem) -> SharedArray | np.ndarray:
        shared_array = obj.array
        size = shared_array._shared_memory.size

        if obj.copy:
            a = shared_array.copy_array()
            if obj.delete:
                shared_array.close()
                shared_array.unlink()
        else:
            a = shared_array

        self._n_bytes_add(-size)
        return a

    @deserialize.register(tuple)
    def _deserialize(self, obj: ArrayQueueItem) -> tuple:
        return tuple(self.deserialize(item) for item in obj)

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
        return self.deserialize(super().get(block=block, timeout=timeout))

    async def get_async(self, block: bool = True, timeout: float | None = None, interval: float = 0.0) -> Any:
        """Asynchronously gets an item from the queue, waits for an item if the queue is empty.

        Args:
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for an item in the queue.
            interval: The time, in seconds, between each queue check.

        Returns:
            The requested item.

        Raises:
            Empty: When there are no items to get in the queue when not blocking
            InterruptedError: When this method is interrupted by the interrupt event.
        """
        return self.deserialize(await super().get_async(block=block, timeout=timeout, interval=interval))

    def put(self, obj: Any) -> None:
        """Puts an item from on the queue."""
        super().put(self.serialize(obj))

    async def put_async(self, obj: Any, timeout: float | None = None, interval: float = 0.0) -> None:
        """Asynchronously puts an object into the queue, waits for access to the queue.

        Args:
            obj: The object to put into the queue.
            timeout: The time, in seconds, to wait for access to the queue.
            interval: The time, in seconds, between each access check.
        """
        await super().put_async(await self.serialize_async(obj), timeout=timeout, interval=interval)
