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
import pytest

# Local Packages #
from src.taskblocks import TaskBlock, AsyncEvent, SimpleAsyncQueue


# Definitions #
# Classes #
class TestTask:
    class ExampleTaskBlock(TaskBlock):

        def construct_io(self) -> None:
            self.inputs.queues["main_input"] = SimpleAsyncQueue()

            self.outputs.events["setup_check"] = AsyncEvent()
            self.outputs.events["teardown_check"] = AsyncEvent()
            self.outputs.queues["main_output"] = SimpleAsyncQueue()

        def link_inputs(self, *args: Any, **kwargs: Any) -> None:
            pass

        def setup(self, *args: Any, **kwargs: Any) -> None:
            self.outputs.events["setup_check"].set()

        async def task(self, *args: Any, **kwargs: Any) -> None:
            try:
                in_item = await self.inputs.queues["main_input"].get_async()
            except InterruptedError:
                return

            out_item = in_item + 1
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
        task.inputs.queues["main_input"].put(1)

        task.run()
        task.join()

        assert task.outputs.events["setup_check"].is_set()
        assert task.outputs.queues["main_output"].get() == 2
        assert task.outputs.events["teardown_check"].is_set()

    def test_run_in_async_loop(self):
        task = self.ExampleTaskBlock()
        task.inputs.queues["main_input"].put(1)

        asyncio.run(self.in_async_loop(task))
        task.join()

        assert task.outputs.events["setup_check"].is_set()
        assert task.outputs.queues["main_output"].get() == 2
        assert task.outputs.events["teardown_check"].is_set()

    def test_start_process(self):
        task = self.ExampleTaskBlock(is_process=True)

        task.start()
        assert task.outputs.events["setup_check"].wait()

        task.inputs.queues["main_input"].put(1)
        assert task.outputs.queues["main_output"].get() == 2

        task.inputs.queues["main_input"].put(2)
        assert task.outputs.queues["main_output"].get() == 3

        task.stop()
        assert task.outputs.events["teardown_check"].wait()

        task.join()




# Main #
if __name__ == "__main__":
    pytest.main(["-v", "-s"])
