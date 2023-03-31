""" asyncevent.py

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
from multiprocessing.synchronize import Event
from multiprocessing.context import BaseContext
import time

# Third-Party Packages #

# Local Packages #


# Definitions #
# Classes #
class AsyncEvent(Event):
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *, ctx: BaseContext | None = None) -> None:
        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    def __bool__(self) -> bool:
        return self.is_set()

    # Instance Methods #
    async def wait_event_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Event | None = None,
    ) -> bool:
        interrupt = interrupt or Event(ctx=get_context())
        if timeout is None:
            while not interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return True
                await asyncio.sleep(interval)
        else:
            deadline = time.perf_counter() + timeout
            while not interrupt.is_set():
                with self._cond:
                    if self._flag.acquire(False):
                        self._flag.release()
                        return True

                if deadline <= time.perf_counter():
                    raise TimeoutError

                await asyncio.sleep(interval)

        # Interruption leads to an error.
        raise InterruptedError
