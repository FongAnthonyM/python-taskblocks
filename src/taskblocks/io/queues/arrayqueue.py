""" arrayqueue.py
A queue for sending numpy ndarrays and other objects to other processes.

This queue extends an AsyncQueue with handling ndarrays. A ndarray can take a large amount of memory and default
queues create a pickle copy of the array to enqueue, which is slow and takes up diskspace. To be faster and prevent
large disk read-writes, this queue instead passes SharedMemory containing the arrays to other processes.

When a ndarrays is passed directly to into the "put" methods, a copy of the array is created in SharedMemory and,
with its "get" handling instructions, it is serialized into an ArrayQueueItem, which will be put into the queue.

An ArrayQueueItem consist of the SharedMemory of the array (SharedArray), a boolean for if it will be copied into a
normal ndarray when "get" from the queue, a boolean for if the SharedArray will be deleted from memory when "get"
from the queue, and a boolean for if the ArrayQueueItem will be returned instead.

Instead of passing a ndarray, other objects can be passed to control how the array is handled in the "get" method.
When a SharedArray is passed the SharedArray is returned from the "get" method without copying or deleting.
Alternatively, an ArrayQueueItem can be passed directly for user defined control over the "get" handling. Lastly, a
tuple of objects can be passed to the "put" methods which will serialized all objects recursively and that the tuple
of serialized objects will be put into the queue. Also, any objects of other types can be passed into the "put"
methods or serialization as well, but they will put into the queue normally with any special handling.
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
from asyncio import sleep, gather
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
from ..sharedmemory import SharedArray, SharedMemoryRegister
from .asyncqueue import AsyncQueue


# Definitions #
# Classes #
class ArrayQueueItem(NamedTuple):
    """An object which holds a SharedArray and its queue "get" deserialization information.

    Args:
        array: The SharedArray.
        copy: Determines if the SharedArray will be copied to a normal ndarray upon deserialization.
        delete: Determines if the SharedArray will be deleted after deserialization.
        as_item: Determines if this object will be returned rather than deserialized.
    """

    array: SharedArray
    copy: bool
    delete: bool
    as_item: bool = True


class ArrayQueue(AsyncQueue):
    """A queue for sending numpy ndarrays and other objects to other processes.

    This queue extends an AsyncQueue with handling ndarrays. A ndarray can take a large amount of memory and default
    queues create a pickle copy of the array to enqueue, which is slow and takes up diskspace. To be faster and prevent
    large disk read-writes, this queue instead passes shared_memories of SharedMemory containing the arrays to other processes.

    When a ndarrays is passed directly to into the "put" methods, a copy of the array is created in SharedMemory and,
    with its "get" handling instructions, it is serialized into an ArrayQueueItem, which will be put into the queue.

    An ArrayQueueItem consist of the SharedMemory of the array (SharedArray), a boolean for if it will be copied into a
    normal ndarray when "get" from the queue, a boolean for if the SharedArray will be deleted from memory when "get"
    from the queue, and a boolean for if the ArrayQueueItem will be returned instead.

    Instead of passing a ndarray, other objects can be passed to control how the array is handled in the "get" method.
    When a SharedArray is passed the SharedArray is returned from the "get" method without copying or deleting.
    Alternatively, an ArrayQueueItem can be passed directly for user defined control over the "get" handling. Lastly, a
    tuple of objects can be passed to the "put" methods which will serialized all objects recursively and that the tuple
    of serialized objects will be put into the queue. Also, any objects of other types can be passed into the "put"
    methods or serialization as well, but they will put into the queue normally with any special handling.

    Attributes:
        add_interrupt: Interrupts blocking add to n_bytes methods.
        _maxbytes: The max number of bytes that can be in the queue.
        _n_bytes: The number of bytes in this queue.
        bytes_wait: Determines if this queue will wait for the byte-space to enqueue an item.
        _shared_registry: The register for SharedMemories being sent on the queue to keep them alive while on the queue.
        _unprocessed: The number of items that have not processed from the queue. This prevents premature deletion.

    Args:
        maxsize: The maximum number items that can be in the queue.
        maxbytes: The maximum number bytes that can be in the queue.
        bytes_wait: Determines if this queue will wait for the byte-space to enqueue an item.
        ctx: The context for the Python multiprocessing.
    """

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        maxsize: int = 0,
        maxbytes: int = 10**10,  # 10 Gigabytes
        bytes_wait: bool = False,
        *,
        ctx: BaseContext | None = None,
    ) -> None:
        # New Attributes #
        self.add_interrupt: Interrupt = Interrupt()
        self._maxbytes: Value = Value("q")
        self._n_bytes: Value = Value("q")

        self.bytes_wait: bool = bytes_wait

        self._shared_registry: SharedMemoryRegister = SharedMemoryRegister()
        self._unprocessed: Value = Value("q")

        # Construction #
        super().__init__(maxsize=maxsize, ctx=ctx)
        self.maxbytes = maxbytes
        with self._n_bytes:
            self._n_bytes.value = 0

        with self._unprocessed:
            self._unprocessed.value = 0

    @property
    def maxbytes(self) -> int:
        """The max number of bytes that can be in the queue."""
        with self._maxbytes:
            return self._maxbytes.value

    @maxbytes.setter
    def maxbytes(self, value: int) -> None:
        with self._maxbytes:
            self._maxbytes.value = value

    @property
    def n_bytes(self) -> int:
        """n_bytes: The number of bytes in this queue."""
        with self._n_bytes:
            return self._n_bytes.value

    # Instance Methods #
    def bytes_space_check(self, size: int) -> bool:
        """Checks if there is enough bytes-space in the queue to add a given size.

        Args:
            size: The number bytes to see if it will fit into the queue.

        Returns:
            If there is enough
        """
        return size + self.n_bytes <= self.maxbytes

    def _n_bytes_add(self, i: int) -> None:
        """Safely adds numbers to the n_bytes.

        Args:
            i: The number of bytes to add to this queue.
        """
        with self._n_bytes:
            self._n_bytes.value += i

    def _add_bytes(self, size: int, block: bool = True, timeout: float | None = None) -> None:
        """Adds bytes to the queue total if there is enough bytes-space.

        Args:
            size: The number of bytes to add to this queue.
            block: Determines if this method will block execution to wait for enough bytes-space to add bytes.
            timeout: The time, in seconds, to wait for enough bytes-space to add.

        Raises:
            Full: When there is not enough bytes-space to add to the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by the interrupt event.
        """
        # Try to add bytes without blocking.
        if not block:
            if self._n_bytes.acquire(block=False):
                try:
                    if size > self.maxbytes:
                        raise ValueError("Value is to large for queue")
                    elif size + self._n_bytes.value <= self.maxbytes:
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
                        if size > self.maxbytes:
                            raise ValueError("Value is to large for queue")
                        elif size + self._n_bytes.value <= self.maxbytes:
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
        """Asynchronously adds bytes to the queue total if there is enough bytes-space.

        Args:
            size: The number of bytes to add to this queue.
            block: Determines if this method will block execution to wait for enough bytes-space to add bytes.
            timeout: The time, in seconds, to wait for enough bytes-space to add.
            interval: The time, in seconds, between each bytes check.

        Raises:
            Full: When there is not enough bytes-space to add to the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by the interrupt event.
        """
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
                        if size > self.maxbytes:
                            raise ValueError("Value is to large for queue")
                        elif size + self._n_bytes.value <= self.maxbytes:
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
                        if size > self.maxbytes:
                            raise ValueError("Value is to large for queue")
                        elif size + self._n_bytes.value <= self.maxbytes:
                            self._n_bytes.value += size
                            return
                    finally:
                        self._n_bytes.release()
                if deadline is not None and deadline <= perf_counter():
                    raise Full
                await sleep(interval)

        raise InterruptedError

    def update_registry(self) -> None:
        """Updates the registry by ensuring the number of registered arrays is less than the number of queue items."""
        memories = self._shared_registry.shared_memories
        with self._unprocessed:
            unprocessed = self._unprocessed.value

        for n in list(memories)[0 : len(memories) - unprocessed]:
            memories.pop(n)

    # Serialization
    @singlekwargdispatch(kwarg="obj")
    def serialize(self, obj: Any, block: bool = True) -> Any:
        """Serialize an object, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        return obj

    @serialize.register(np.ndarray)
    def _serialize(self, obj: np.ndarray, block: bool = True) -> ArrayQueueItem:
        """Serialize a ndarray, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        self._add_bytes(obj.nbytes, block=(block or self.bytes_wait))
        a = SharedArray(a=obj, register=False)
        self._shared_registry.register_shared_memory(a)
        with self._unprocessed:
            self._unprocessed.value += 1
        return ArrayQueueItem(a, copy=True, delete=True, as_item=False)

    @serialize.register(SharedArray)
    def _serialize(self, obj: SharedArray, block: bool = True) -> ArrayQueueItem:
        """Serialize a SharedArray, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        self._add_bytes(obj._shared_memory.size, block=(block or self.bytes_wait))
        self._shared_registry.register_shared_memory(obj)
        with self._unprocessed:
            self._unprocessed.value += 1
        return ArrayQueueItem(obj, copy=False, delete=False, as_item=False)

    @serialize.register(ArrayQueueItem)
    def _serialize(self, obj: ArrayQueueItem, block: bool = True) -> ArrayQueueItem:
        """Serialize a ArrayQueueItem, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        self._add_bytes(obj.array._shared_memory.size, block=(block or self.bytes_wait))
        self._shared_registry.register_shared_memory(obj.array)
        with self._unprocessed:
            self._unprocessed.value += 1
        return obj

    @serialize.register(tuple)
    def _serialize(self, obj: tuple, block: bool = True) -> tuple:
        """Serialize a tuple by serializing all its items, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        return tuple(self.serialize(item, block=block) for item in obj)

    # Serialization Async
    @singlekwargdispatch(kwarg="obj")
    async def serialize_async(self, obj: Any, block: bool = True) -> Any:
        """Asynchronously serialize an object, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        return obj

    @serialize_async.register(np.ndarray)
    async def _serialize_async(self, obj: np.ndarray, block: bool = True) -> ArrayQueueItem:
        """Asynchronously serialize a ndarray, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        await self._add_bytes_async(obj.nbytes, block=(block or self.bytes_wait))
        a = SharedArray(a=obj, register=False)
        self._shared_registry.register_shared_memory(a)
        with self._unprocessed:
            self._unprocessed.value += 1
        return ArrayQueueItem(a, copy=True, delete=True, as_item=False)

    @serialize_async.register(SharedArray)
    async def _serialize_async(self, obj: SharedArray, block: bool = True) -> ArrayQueueItem:
        """Asynchronously serialize a SharedArray, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        await self._add_bytes_async(obj._shared_memory.size, block=(block or self.bytes_wait))
        self._shared_registry.register_shared_memory(obj)
        with self._unprocessed:
            self._unprocessed.value += 1
        return ArrayQueueItem(obj, copy=False, delete=False, as_item=False)

    @serialize_async.register(ArrayQueueItem)
    async def _serialize_async(self, obj: ArrayQueueItem, block: bool = True) -> ArrayQueueItem:
        """Asynchronously serialize an ArrayQueueItem, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        await self._add_bytes_async(obj.array._shared_memory.size, block=(block or self.bytes_wait))
        self._shared_registry.register_shared_memory(obj.array)
        with self._unprocessed:
            self._unprocessed.value += 1
        return obj

    @serialize_async.register(tuple)
    async def _serialize_async(self, obj: tuple, block: bool = True) -> tuple:
        """Asynchronously serialize a tuple by serializing all its items, so it can be put into the queue.

        Args:
            obj: The object to serialize.
            block: Determines if this method will block while waiting for bytes space in the the queue.

        Returns:
            An object to add to the queue.
        """
        return tuple(await gather(*(self.serialize_async(item, block=block) for item in obj)))

    # Deserialization
    @singlekwargdispatch("obj")
    def deserialize(self, obj: Any) -> Any:
        """Deserialize an object from the queue.

        Args:
            obj: The object to deserialize.

        Returns:
            An object from the queue.
        """
        return obj

    @deserialize.register(ArrayQueueItem)
    def _deserialize(self, obj: ArrayQueueItem) -> ArrayQueueItem | SharedArray | np.ndarray:
        """Deserialize an ArrayQueueItem from the queue.

        Args:
            obj: The object to deserialize.

        Returns:
            An object from the queue.
        """
        shared_array = obj.array
        size = shared_array.nbytes

        if obj.as_item:
            a = obj
        elif obj.copy:
            a = shared_array.copy_array()
            if obj.delete:
                shared_array.close()
                shared_array.unlink()
        else:
            a = shared_array

        self._n_bytes_add(-size)
        with self._unprocessed:
            self._unprocessed.value -= 1
        return a

    @deserialize.register(tuple)
    def _deserialize(self, obj: tuple) -> tuple:
        """Deserialize a tuple of objects from the queue.

        Args:
            obj: The objects to deserialize.

        Returns:
            A tuple of objects from queue.
        """
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

    def put(self, obj: Any, block: bool = True, timeout: float | None = None) -> None:
        """Puts an item from on the queue.

        Args:
            obj: The object to put into the queue.
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for space in the queue.
        """
        super().put(self.serialize(obj), block=block, timeout=timeout)
        self.update_registry()

    async def put_async(self, obj: Any, timeout: float | None = None, interval: float = 0.0) -> None:
        """Asynchronously puts an object into the queue, waits for access to the queue.

        Args:
            obj: The object to put into the queue.
            timeout: The time, in seconds, to wait for access to the queue.
            interval: The time, in seconds, between each access check.
        """
        await super().put_async(await self.serialize_async(obj), timeout=timeout, interval=interval)
        self.update_registry()

    def join(self) -> None:
        """Blocks until all items in the Queue have been gotten and the registry is updated."""
        previous = self.qsize()
        while current := self.qsize() > 0:
            if current < previous:
                self.update_registry()
            previous = current
        self.update_registry()

    async def join_async(self, interval: float = 0.0) -> None:
        """Asynchronously, blocks until all items in the Queue have been gotten and the registry is updated.

        Args:
            interval: The time, in seconds, between each queue check.
        """
        previous = self.qsize()
        while current := self.qsize() > 0:
            if current < previous:
                self.update_registry()
            previous = current
            await sleep(interval)
        self.update_registry()
