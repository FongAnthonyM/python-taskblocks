""" simpleasyncqueue.py

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
from multiprocessing import get_context
from multiprocessing.queues import SimpleQueue
from multiprocessing.context import BaseContext
from multiprocessing.reduction import ForkingPickler
from queue import Empty
import time
from typing import Any

# Third-Party Packages #

# Local Packages #
from .interrupt import Interrupt


# Definitions #
# Classes #
class SimpleAsyncQueue(SimpleQueue):
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.get_interrupt: Interrupt | None = None
        self.put_interrupt: Interrupt | None = None

        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    def __del__(self) -> None:
        self.close()

    # Instance Methods #
    def get(self, block=True, timeout=None):
        if block and timeout is None:
            with self._rlock:
                res = self._recv_bytes()
        else:
            if block:
                deadline = time.perf_counter() + timeout
            if not self._rlock.acquire(block, timeout):
                raise Empty
            try:
                if block:
                    timeout = deadline - time.perf_counter()
                    if not self._reader.poll(timeout):
                        raise Empty
                elif not self._reader.poll():
                    raise Empty
                res = self._recv_bytes()
            finally:
                self._rlock.release()

        # unserialize the data after having released the lock
        return ForkingPickler.loads(res)

    async def get_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> Any:
        self.get_interrupt = Interrupt() if interrupt is None else interrupt
        res = None
        deadline = None if timeout is None else time.perf_counter() + timeout
        while not self.get_interrupt.is_set():
            if self._rlock.aqcuire(block=False):
                try:
                    if self._reader.poll():
                        res = self._reader.recv_bytes()
                        break
                finally:
                    self._rlock.release()
            elif deadline is not None and deadline <= time.perf_counter():
                raise Empty

            await asyncio.sleep(interval)

        if res is not None:
            # unserialize the data after having released the lock
            return ForkingPickler.loads(res)
        else:
            raise InterruptedError

    def put_bytes(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        # serialize the data before acquiring the lock
        if self._wlock is None:
            # writes to a message oriented win32 pipe are atomic
            self._writer.send_bytes(buf, offset, size)
        else:
            with self._wlock:
                self._writer.send_bytes(buf, offset, size)

    async def put_bytes_async(
        self,
        buf: bytes,
        offset: int = 0,
        size: int | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> None:
        self.put_interrupt = interrupt or Interrupt()
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

                await asyncio.sleep(interval)
        else:
            deadline = time.perf_counter() + timeout
            while not self.put_interrupt.is_set():
                if self._wlock.acquire(block=False):
                    try:
                        self._writer.send_bytes(buf, offset, size)
                        return
                    finally:
                        self._wlock.release()
                if deadline <= time.perf_counter():
                    raise TimeoutError

                await asyncio.sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError

    async def put_async(
        self,
        obj: Any,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> None:
        await self.put_bytes_async(ForkingPickler.dumps(obj), timeout=timeout, interval=interval, interrupt=interrupt)
