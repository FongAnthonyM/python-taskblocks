#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" test_singlekwargdispatchmethod.py
Tests singlekwargdispatchmethod
"""
# Package Header #
from src.taskblocks.header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
import asyncio
from typing import Any

# Third-Party Packages #
import numpy as np
import pytest

# Local Packages #
from src.taskblocks import TaskBlock, AsyncEvent
from src.taskblocks.io.sharedmemory import SharedArray
from src.taskblocks.io.queues.arrayqueue import ArrayQueue


# Definitions #
# Classes #
class TestArrayQueue:
    class ExampleTaskBlock(TaskBlock):
        def construct_io(self) -> None:
            self.outputs.events["setup_check"] = AsyncEvent()
            self.outputs.events["teardown_check"] = AsyncEvent()
            self.outputs.queues["main_output"] = ArrayQueue()

        def setup(self, *args: Any, **kwargs: Any) -> None:
            self.outputs.events["setup_check"].set()

        async def task(self, *args: Any, **kwargs: Any) -> None:

            out_item = (np.ones((100, 100)),)
            await self.outputs.queues["main_output"].put_async(out_item)

        def teardown(self, *args: Any, **kwargs: Any) -> None:
            self.outputs.events["teardown_check"].set()

        def stop(self) -> None:
            """Stops the task."""
            super().stop()
            self.inputs.queues["main_input"].get_interrupt.set()

    async def in_async_loop(self, task):
        task.run()

    def test_run(self):
        task = self.ExampleTaskBlock()

        task.run()
        task.join()

        assert task.outputs.events["setup_check"].is_set()
        assert task.outputs.queues["main_output"].get_single() == 2
        assert task.outputs.events["teardown_check"].is_set()

    def test_run_in_async_loop(self):
        task = self.ExampleTaskBlock()
        task.inputs.queues["main_input"].put_single(
            1,
        )

        asyncio.run(self.in_async_loop(task))
        task.join()

        assert task.outputs.events["setup_check"].is_set()
        assert task.outputs.queues["main_output"].get_single() == 2
        assert task.outputs.events["teardown_check"].is_set()

    def test_start_process(self):
        task = self.ExampleTaskBlock(is_process=True)

        task.start()
        assert task.outputs.events["setup_check"].wait()

        task.inputs.queues["main_input"].put_single(
            1,
        )
        assert task.outputs.queues["main_output"].get_single() == 2

        task.inputs.queues["main_input"].put_single(
            2,
        )
        assert task.outputs.queues["main_output"].get_single() == 3

        task.stop()
        assert task.outputs.events["teardown_check"].wait()

        task.join()

    def test_put_and_get_copy(self):
        q = ArrayQueue()

        a = np.ones((100000, 10000))
        a_size = a.nbytes

        q.put(a)

        held_size = q.n_bytes

        new_a = q.get()
        empty_size = q.n_bytes
        q.close()

        assert a_size == held_size
        assert (a == new_a).all()
        assert empty_size == 0

    def test_put_and_get_no_copy(self):
        q = ArrayQueue()

        data = np.ones((100000, 10000))
        a = SharedArray(data)
        a_size = a.nbytes

        q.put(a)

        held_size = q.n_bytes

        new_a = q.get()
        empty_size = q.n_bytes
        new_a

        q.close()
        a.close()
        a.unlink()

        assert a_size == held_size
        assert empty_size == 0


# Main #
if __name__ == "__main__":
    pytest.main(["-v", "-s"])
