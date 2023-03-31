""" simpleasxyncqueuemanager.py

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
import asyncio
from collections.abc import Iterable
from multiprocessing.reduction import ForkingPickler
import time
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject

# Local Packages #
from .interrupt import Interrupt
from .simpleasyncqueue import SimpleAsyncQueue


# Definitions #
# Classes #
class SimpleAsyncQueueManager(BaseObject):
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        names: Iterable[str] | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.get_interrupt: Interrupt | None = None

        self.queues: dict[str, SimpleAsyncQueue] = {}

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construction #
        if init:
            self.construct(names=names, *args, **kwargs)

    # Instance Methods #
    # Constructors/Destructors
    def construct(self, names: Iterable[str] | None = None, *args: Any, **kwargs: Any) -> None:
        """Constructs this object.

        Args:
            names: Names of pipes to create.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if names is not None:
            self.create_queues(names=names)

        super().construct(*args, **kwargs)

    # Queue
    def create_queue(self, name: str):
        self.queues[name] = out = SimpleAsyncQueue()
        return out

    def create_queues(self, names: Iterable[str]):
        for name in names:
            self.queues[name] = SimpleAsyncQueue()

    def set_queue(self, name: str, q):
        self.queues[name] = q

    def get_queue(self, name):
        return self.queues[name]

    # Object Query
    def empty(self):
        return {k: q.empty() for k, q in self.queues.items()}

    def all_empty(self):
        for q in self.queues.values():
            if not q.empty():
                return False
        return True

    def any_empty(self):
        for q in self.queues.values():
            if q.empty():
                return True
        return True

    # Get
    def get(self, name: str, block: bool = True, timeout: float | None = 0.0) -> Any:
        return self.queues[name].get(block=block, timeout=timeout)

    async def get_async(
        self,
        name: str,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> Any:
        return await self.queues[name].get_async(timeout, interval, interrupt)

    def get_all(self, block: bool = True, timeout: float | None = 0.0) -> dict[str, Any]:
        return {n: q.get(block=block, timeout=timeout) for n, q in self.queues.items()}

    async def get_all_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> Any:
        if timeout is None:
            return {n: await q.get_async(interval=interval, interrupt=interrupt) for n, q in self.queues.items()}
        else:
            items = {}
            deadline = time.perf_counter() + timeout
            for n, q in self.queues.items():
                if deadline <= time.perf_counter():
                    raise TimeoutError

                items[n] = await q.get_async(interval=interval, interrupt=interrupt)

    def get_all_tasks(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> Any:
        return {n: asyncio.create_task(q.get_async(timeout=timeout, interval=interval, interrupt=interrupt))
                for n, q in self.queues.items()}

    # Put
    def put(self, name: str, obj: Any) -> None:
        self.queues[name].put(obj)

    def put_bytes_all(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        for q in self.queues.values():
            q.put_bytes(buf, offset, size)

    def put_all(self, obj):
        self.put_bytes_all(ForkingPickler.dumps(obj))

    async def put_bytes_all_async(
        self,
        buf: bytes,
        offset: int = 0,
        size: int | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> None:
        if timeout is None:
            for q in self.queues.values():
                await q.put_bytes_async(buf, offset, size, interval=interval, interrupt=interrupt)
        else:
            deadline = time.perf_counter() + timeout
            for q in self.queues.values():
                if deadline <= time.perf_counter():
                    raise TimeoutError

                await q.put_bytes_async(buf, offset, size, interval=interval, interrupt=interrupt)

    async def put_all_async(
        self,
        obj: Any,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> None:
        await self.put_bytes_all_async(
            ForkingPickler.dumps(obj),
            timeout=timeout,
            interval=interval,
            interrupt=interrupt,
        )
