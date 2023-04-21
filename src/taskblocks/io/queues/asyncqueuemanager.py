""" simpleasxyncqueuemanager.py
A manager for several SimpleAsyncQueues. Has methods for sending and receiving data on all queues.
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
from asyncio import Task, create_task
from collections.abc import Iterable, Iterator
from itertools import cycle
from multiprocessing.reduction import ForkingPickler
from time import perf_counter
from typing import Any

# Third-Party Packages #
from baseobjects.functions import MethodMultiplexer
from baseobjects.collections import OrderableDict

# Local Packages #
from .asyncqueueinterface import AsyncQueueInterface
from .simpleasyncqueue import SimpleAsyncQueue
from .asyncqueue import AsyncQueue


# Definitions #
# Static #
SENTINEL = object()


# Classes #
class AsyncQueueManager(AsyncQueueInterface):
    """A manager for several SimpleAsyncQueues. Has methods for sending and receiving data on all queues.

    Attributes:
        primary_queue: The name of the queue to use by default in get/put methods.
        queues: The queues contained within this manager.
        get: The get method which can be set to another method within this object.
        get_async: The set_async method which can be set to another method within this object.
        put: The put method which can be set to another method within this object.
        put_async: The put_async method which can be set to another method within this object.

    Args:
        queues: The queues to add to this manager.
        names: The names of queues to create.
        primary: The name of the queue to use by default in get/put methods.
        *args: Arguments for inheritance.
        init: Determines if this object should be initialized.
        **kwargs: Keyword arguments for inheritance.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        queues: dict[str, Any] | None = None,
        primary: str | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.primary_queue: str = ""
        
        self.queues: OrderableDict[str, AsyncQueueInterface] = OrderableDict()
        self._queue_cycle: cycle | None = None

        self.put_bytes: MethodMultiplexer = MethodMultiplexer(instance=self, select="put_bytes_all")
        self.put_bytes_async: MethodMultiplexer = MethodMultiplexer(instance=self, select="put_bytes_all_async")
        
        self.get: MethodMultiplexer = MethodMultiplexer(instance=self, select="get_all")
        self.get_async: MethodMultiplexer = MethodMultiplexer(instance=self, select="get_all_async")
        
        self.put: MethodMultiplexer = MethodMultiplexer(instance=self, select="put_all")
        self.put_async: MethodMultiplexer = MethodMultiplexer(instance=self, select="put_all_async")

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construction #
        if init:
            self.construct(queues=queues, primary=primary, *args, **kwargs)
            
    @property
    def queue_cycle(self) -> cycle:
        """A cycle of the contained queues"""
        if self._queue_cycle is None:
            self._queue_cycle = cycle(self.queues.values())
        return self._queue_cycle

    # Container Methods
    def __getitem__(self, key: str) -> AsyncQueueInterface:
        """Gets a queue in this object."""
        return self.queues[key]

    def __setitem__(self, key: str, value: AsyncQueueInterface) -> None:
        """Sets a queue in this object."""
        self.queues[key] = value

    def __delitem__(self, key: str) -> None:
        """Deletes an item from this object."""
        del self.queues[key]

    def __iter__(self) -> Iterator[AsyncQueueInterface]:
        """Returns an iterator for the queues."""
        return iter(self.queues.values())

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self, 
        queues: dict[str, Any] | None = None,
        primary: str | None = None,
        *args: Any, 
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            queues: The queues to add to this manager.
            primary: The name of the queue to use by default in get/put methods.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if queues is not None:
            self.queues.update(queues)
            
        if primary is not None:
            self.primary_queue = primary

        super().construct(*args, **kwargs)

    # Queue
    def get_queue(self, name: str, default: Any = SENTINEL) -> Any:
        """Gets a queue from this manager.

        Args:
            name: The name of the SimpleAsyncQueues to get.
            default: The default value to return if key is not found

        Returns
            The requested queue manage.
        """
        if default is not SENTINEL:
            return self.queues[name]
        else:
            return self.queues.get(name, default)

    def get_queue_index(self, index: int, default: Any = SENTINEL) -> Any:
        """Gets a queue synchronize on its index in the order.

        Args:
            index: The index of the queue.
            default: The value to return if the index is outside the range.

        Returns:
            The requested value.
        """
        if default is not SENTINEL:
            return self.queues.get_index(index, default)
        else:
            return self.queues.get_index(index, default)

    def set_queue(self, name: str, q: Any) -> None:
        """Sets a queue in this manager.

        Args:
            name: The name of the SimpleAsyncQueues to set.
            q: The queue to manage.
        """
        self.queues[name] = q

    def insert_queue(self, index: int, name: str, q: Any) -> None:
        """Inserts a queue in this manager, raises error if it already exists.

        Args:
            index: The order index to set the queue to.
            name: The name of the SimpleAsyncQueues to set.
            q: The queue to manage.
        """
        self.queues.insert(index, name, q)

    def insert_move_queue(self, index: int, name: str, q: Any) -> None:
        """Inserts a queue in this manager, moves the queue to the order index if it already exists.

        Args:
            index: The order index to set the queue to.
            name: The name of the SimpleAsyncQueues to set.
            q: The queue to manage.
        """
        self.queues.insert(index, name, q)

    # Object Query
    def empty(self) -> dict[str, bool]:
        """Returns a dictionary with all results of an empty check for each queue.
        
        Returns:
            All the queues "is empty" state.
        """
        return {k: q.empty() for k, q in self.queues.items()}

    def all_empty(self) -> bool:
        """Checks if all the queues are empty.
        
        Returns:
            If all the queues are empty.
        """
        return all((q.empty() for q in self.queues.values()))

    def any_empty(self) -> bool:
        """Checks if any the queues are empty.

        Returns:
            If any the queues are empty.
        """
        return any((q.empty() for q in self.queues.values()))

    # Get
    def get_single(self, name: str | None = None, timeout: float | None = 0.0) -> Any:
        """Gets an item from a queue, waits for an item if a queue is empty.

        Args:
            name: The queue to get an item from.
            timeout: The time, in seconds, to wait for an item in the queue.
        
        Returns:
            The requested item.
        """
        return self.queues[name or self.primary_queue].get(timeout=timeout)

    async def get_single_async(
        self,
        name: str | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Any:
        """Asynchronously gets an item from a queue, waits for an item if a queue is empty.

        Args:
            name: The queue to get an item from.
            timeout: The time, in seconds, to wait for an item in the queue.
            interval: The time, in seconds, between each queue check.
        
        Returns:
            The requested item.
        """
        return await self.queues[name or self.primary_queue].get_async(timeout, interval)

    def get_all(self, timeout: float | None = 0.0) -> dict[str, Any]:
        """Gets an item from all queues, waits for an item if a queue is empty.

        Args:
            timeout: The time, in seconds, to wait for an item in the queue.
        
        Returns:
            The requested items.
        """
        return {n: q.get(timeout=timeout) for n, q in self.queues.items()}

    async def get_all_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Any:
        """Asynchronously gets an items from all queues, waits for an item if a queue is empty.

        Args:
            timeout: The time, in seconds, to wait for an item in each queue.
            interval: The time, in seconds, between each queue check.
        
        Returns:
            The requested items.

        Raises:
            TimeoutError: When this method is unable to complete within the timeout time.
        """
        if timeout is None:
            return {n: await q.get_async(interval=interval) for n, q in self.queues.items()}
        else:
            items = {}
            deadline = perf_counter() + timeout
            for n, q in self.queues.items():
                if deadline <= perf_counter():
                    raise TimeoutError

                items[n] = await q.get_async(interval=interval)
        
            return items

    def get_all_tasks(self, timeout: float | None = None, interval: float = 0.0) -> dict[str, Task]:
        """Asynchronously gets all items from each queue, but returns awaitable tasks for each queue.

        Args:
            timeout: The time, in seconds, to wait for an item in each queue.
            interval: The time, in seconds, between each queue check.

        Returns:
            Awaitable tasks for each queue.
        """
        return {n: create_task(q.get_async(timeout=timeout, interval=interval)) for n, q in self.queues.items()}

    # Put Bytes
    def put_bytes_single(self, buf: bytes, offset: int = 0, size: int | None = None, name: str | None = None) -> None:
        """Puts a bytes object into a queue, waits for access to the queue.

        Args:
            buf: The bytes buffer to put into the queue.
            offset: The offset in the buffer to put the bytes put into the queue.
            size: The amount of the bytes to put into the queue.
            name: The queue to put the bytes into.
        """
        self.queues[name or self.primary_queue].put_bytes(buf, offset, size)

    async def put_bytes_single_async(
        self,
        buf: bytes,
        offset: int = 0,
        size: int | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
        name: str | None = None,
    ) -> None:
        """Asynchronously puts a bytes object into a queue, waits for access to each queue.

        Args:
            buf: The bytes buffer to put into each queue.
            offset: The offset in the buffer to put the bytes put into each queue.
            size: The amount of the bytes to put into each queue.
            timeout: The time, in seconds, to wait for access to each queue.
            interval: The time, in seconds, between each access check.
            name: The queue to put the bytes into.
        """
        await self.queues[name or self.primary_queue].put_bytes(buf, offset, size, timeout, interval)

    def put_bytes_all(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        """Puts a bytes object into all queues, waits for access to each queue.

        Args:
            buf: The bytes buffer to put into each queue.
            offset: The offset in the buffer to put the bytes put into each queue.
            size: The amount of the bytes to put into each queue.
        """
        for q in self.queues.values():
            q.put_bytes(buf, offset, size)
            
    async def put_bytes_all_async(
        self,
        buf: bytes,
        offset: int = 0,
        size: int | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> None:
        """Asynchronously puts a bytes object into all queues, waits for access to each queue.

        Args:
            buf: The bytes buffer to put into each queue.
            offset: The offset in the buffer to put the bytes put into each queue.
            size: The amount of the bytes to put into each queue.
            timeout: The time, in seconds, to wait for access to each queue.
            interval: The time, in seconds, between each access check.
        """
        if timeout is None:
            for q in self.queues.values():
                await q.put_bytes_async(buf, offset, size, interval=interval)
        else:
            deadline = perf_counter() + timeout
            for q in self.queues.values():
                if deadline <= perf_counter():
                    raise TimeoutError

                await q.put_bytes_async(buf, offset, size, interval=interval)
    
    # Put
    def put_single(self, obj: Any, name: str | None = None) -> None:
        """Puts an object into a queue, waits for access to the queue.

        Args:
            obj: The object to put into a queue.
            name: The queue to put an item into.
        """
        self.queues[name or self.primary_queue].put(obj)
        
    async def put_single_async(self, obj: Any, name: str | None = None) -> None:
        """Asynchronously puts an object into a queue, waits for access to the queue.

        Args:
            obj: The object to put into a queue.
            name: The queue to put an item into.
        """
        await self.queues[name or self.primary_queue].put_async(obj)

    def put_cycle(self, obj: Any) -> None:
        """Puts an object into a queue, where each call puts the object into the following queue.

        Args:
            obj: The object to put into a queue.
        """
        next(self.queue_cycle).put(obj)

    async def put_cycle_async(self, obj: Any) -> None:
        """Asynchronously puts an object into a queue, where each call puts the object into the following queue.

        Args:
            obj: The object to put into a queue.
        """
        await next(self.queue_cycle).put_async(obj)

    def put_all(self, obj: Any) -> None:
        """Puts an object into all queue, waits for access to each queue.

        Args:
            obj: The object to put into each queue.
        """
        for q in self.queues.values():
            q.put(obj)

    async def put_all_async(
        self,
        obj: Any,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> None:
        """Asynchronously puts an object into all queues, waits for access to each queue.

        Args:
            obj: The object to put into each queue.
            timeout: The time, in seconds, to wait for access to each queue.
            interval: The time, in seconds, between each access check.
        """
        if timeout is None:
            for q in self.queues.values():
                await q.put_async(obj, interval=interval)
        else:
            deadline = perf_counter() + timeout
            for q in self.queues.values():
                if deadline <= perf_counter():
                    raise TimeoutError

                await q.put_async(obj, interval=interval)
        
    # Interrupt
    def interrupt_all_puts(self) -> None:
        """Interrupts all queues' put calls."""
        for q in self.queues.values():
            q.put_interrupt.set()

    def uninterrupt_all_puts(self) -> None:
        """Clears put interrupts in all queues."""
        for q in self.queues.values():
            q.put_interrupt.clear()
            
    def interrupt_all_gets(self) -> None:
        """Interrupts all queues' get calls."""
        for q in self.queues.values():
            q.get_interrupt.set()

    def uninterrupt_all_gets(self) -> None:
        """Clears get interrupts in all queues."""
        for q in self.queues.values():
            q.get_interrupt.clear()
    
    def interrupt_all(self) -> None:
        """Interrupts all queues."""
        for q in self.queues.values():
            q.put_interrupt.set()
            q.get_interrupt.set()
            
    def uninterrupt_all(self) -> None:
        """Clears all interrupts in all queues."""
        for q in self.queues.values():
            q.put_interrupt.clear()
            q.get_interrupt.clear()
        