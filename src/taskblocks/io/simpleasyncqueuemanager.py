""" simpleasxyncqueuemanager.py
A manager for several SimpleAsyncQueues. Has methods for sending and receiving data on all queues.
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
from asyncio import Task, create_task
from collections.abc import Iterable
from multiprocessing.reduction import ForkingPickler
from time import perf_counter
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject

# Local Packages #
from .simpleasyncqueue import SimpleAsyncQueue


# Definitions #
# Classes #
class SimpleAsyncQueueManager(BaseObject):
    """A manager for several SimpleAsyncQueues. Has methods for sending and receiving data on all queues.

    Attributes:
        queues: The queues contained within this manager.

    Args:
        queues: The queues to add to this manager.
        names: The names of queues to create.
        *args: Arguments for inheritance.
        init: Determines if this object should be initialized.
        **kwargs: Keyword arguments for inheritance.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        queues: dict[str, SimpleAsyncQueue] | None = None,
        names: Iterable[str] | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.queues: dict[str, SimpleAsyncQueue] = {}

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construction #
        if init:
            self.construct(queues=queues, names=names, *args, **kwargs)

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self, 
        queues: dict[str, SimpleAsyncQueue] | None = None, 
        names: Iterable[str] | None = None, 
        *args: Any, 
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            queues: The queues to add to this manager.
            names: The names of queues to create.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if queues is not  None:
            self.queues.update(queues)
        
        if names is not None:
            self.create_queues(names=names)

        super().construct(*args, **kwargs)

    # Queue
    def create_queue(self, name: str) -> SimpleAsyncQueue:
        """Creates a SimpleAsyncQueue to manage.
        
        Args:
            name: The name of the SimpleAsyncQueue to create.
            
        Returns:
            The new SimpleAsyncQueue.
        """
        self.queues[name] = out = SimpleAsyncQueue()
        return out

    def create_queues(self, names: Iterable[str]) -> None:
        """Creates SimpleAsyncQueues to manage.

        Args:
            names: The names of the SimpleAsyncQueues to create.
        """
        for name in names:
            self.queues[name] = SimpleAsyncQueue()

    def set_queue(self, name: str, q: SimpleAsyncQueue) -> None:
        """Sets a queue in this manager.

        Args:
            name: The name of the SimpleAsyncQueues to set.
            q: The queue to manage.
        """
        self.queues[name] = q

    def get_queue(self, name: str) -> SimpleAsyncQueue:
        """Gets a queue from this manager.

        Args:
            name: The name of the SimpleAsyncQueues to get.
            
        Returns
            The requested queue manage.
        """
        return self.queues[name]

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
    def get(self, name: str, timeout: float | None = 0.0) -> Any:
        """Gets an item from a queue, waits for an item if a queue is empty.

        Args:
            name: The queue to get an item from.
            timeout: The time, in seconds, to wait for an item in the queue.
        
        Returns:
            The requested item.
        """
        return self.queues[name].get(timeout=timeout)

    async def get_async(
        self,
        name: str,
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
        return await self.queues[name].get_async(timeout, interval)

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

    # Put
    def put(self, name: str, obj: Any) -> None:
        """Puts an object into the queue, waits for access to the queue.

        Args:
            name: The queue to put an item into.
            obj: The object to put into the queue.
        """
        self.queues[name].put(obj)

    def put_bytes_all(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        """Puts a bytes object into all queues, waits for access to each queue.

        Args:
            buf: The bytes buffer to put into each queue.
            offset: The offset in the buffer to put the bytes put into each queue.
            size: The amount of the bytes to put into each queue.
        """
        for q in self.queues.values():
            q.put_bytes(buf, offset, size)

    def put_all(self, obj: Any) -> None:
        """Puts an object into all queue, waits for access to each queue.

        Args:
            obj: The object to put into each queue.
        """
        self.put_bytes_all(ForkingPickler.dumps(obj))

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
        await self.put_bytes_all_async(
            ForkingPickler.dumps(obj),
            timeout=timeout,
            interval=interval,
        )
        
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
        
    
