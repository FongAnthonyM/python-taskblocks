""" io.py

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
from multiprocessing import context
from warnings import warn
import time

# Third-Party Packages #
from baseobjects import BaseObject
from baseobjects.warnings import TimeoutWarning

# Local Packages #


# Definitions #
_SENTINEL = object()


# Classes #
class SimpleAsyncQueue(SimpleQueue):
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *, ctx: BaseContext | None = None) -> None:
        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    def __del__(self) -> None:
        self.close()

    # Instance Methods #
    def get(self, block=True, timeout=None, sentinel=_SENTINEL):
        if self._rlock.aqcuire(block, timeout):
            try:
                res = self._reader.recv_bytes()
            finally:
                self._rlock.release()

            # unserialize the data after having released the lock
            return context.reduction.ForkingPickler.loads(res)
        else:
            TimeoutWarning()
            return sentinel

    async def get_async(self, timeout=None, interval=0.0, sentinel=_SENTINEL):
        start_time = time.perf_counter()
        while True:
            if self._rlock.aqcuire(block=False):
                try:
                    res = self._reader.recv_bytes()
                finally:
                    self._rlock.release()
                    break
            elif timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return sentinel
            await asyncio.sleep(interval)

        # unserialize the data after having released the lock
        return context.reduction.ForkingPickler.loads(res)

    def put(self, obj, block=True, timeout=None):
        # serialize the data before acquiring the lock
        obj = context.reduction.ForkingPickler.dumps(obj)
        if self._wlock is None:
            # writes to a message oriented win32 pipe are atomic
            self._writer.send_bytes(obj)
        else:
            if self._wlock.acquire(block, timeout):
                try:
                    self._writer.send_bytes(obj)
                finally:
                    self._wlock.release()
            else:
                warnings.warn()

    async def put_async(self, obj, timeout=None, interval=0.0):
        start_time = time.perf_counter()
        # serialize the data before acquiring the lock
        obj = context.reduction.ForkingPickler.dumps(obj)
        if self._wlock is None:
            # writes to a message oriented win32 pipe are atomic
            self._writer.send_bytes(obj)
        else:
            while True:
                if self._wlock.acquire(block=False):
                    try:
                        self._writer.send_bytes(obj)
                    finally:
                        self._wlock.release()
                        break
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
