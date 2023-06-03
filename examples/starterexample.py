# Imports for the example
from datetime import datetime
import time

from taskblocks import TaskBlock, AsyncEvent, AsyncQueue



class ExampleTaskBlock(TaskBlock):

    def construct_io(self) -> None:
        """Constructs the I/O for this TaskBlock."""
        # Inputs
        self.inputs.queues["main_input"] = AsyncQueue()

        # Outputs
        self.outputs.events["setup_check"] = AsyncEvent()
        self.outputs.events["teardown_check"] = AsyncEvent()

        self.outputs.queues["main_output"] = AsyncQueue()

    def setup(self) -> None:
        """This method runs before the task loop only once."""
        # Set an event when this TaskBlock starts
        self.outputs.events["setup_check"].set()

    async def task(self) -> None:
        """This method will run either once or indefinitely depending on if "running" or "starting" the TaskBlock"""
        # Try and get an item from the queue.
        try:
            in_item = await self.inputs.queues["main_input"].get_async()
        except InterruptedError:
            return  # If interrupted, skip this loop of the task.

        print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Received {in_item} in task")

        # Do something to transform the input item to make an output item.
        out_item = in_item + 1

        # Wait to simulate a long compute time
        time.sleep(2)

        # Put the resulting item on the queue.
        await self.outputs.queues["main_output"].put_async(out_item)

    def teardown(self) -> None:
        """This method runs after the task loop only once."""
        # Set an event when this TaskBlock end
        self.outputs.events["teardown_check"].set()

    def stop(self) -> None:
        """Stops the task."""
        super().stop()  # Turn off the task loop.
        self.inputs.queues["main_input"].get_interrupt.set()  # Interrupt the task so it can escape out.


if __name__ == '__main__':
    # Create TaskBlock object
    task = ExampleTaskBlock(is_process=True)


    # Start TaskBlock in separate process and print when setup is complete
    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Setup event is {task.outputs.events['setup_check'].is_set()}")

    task.start()  # Will continuously execute the task until its stop method is called from any process.
    task.outputs.events["setup_check"].wait()

    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Setup event is {task.outputs.events['setup_check'].is_set()}\n")


    # Put some data on the queue and let the TaskBlock process it
    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Putting items on the queue")

    task.inputs.queues["main_input"].put(1)
    task.inputs.queues["main_input"].put(2)

    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Items on the queue")
    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Waiting for results")
    out = task.outputs.queues["main_output"].get()
    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: {out} was returned from the TaskBlock")
    out = task.outputs.queues["main_output"].get()
    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: {out} was returned from the TaskBlock\n")


    # Tells the TaskBlock to stop and print when teardown is complete
    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Teardown event is {task.outputs.events['teardown_check'].is_set()}")

    task.stop()
    task.outputs.events["teardown_check"].wait()

    print(f"{datetime.now().strftime('%H:%M:%S.%f')}: Teardown event is {task.outputs.events['teardown_check'].is_set()}\n")
