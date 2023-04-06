""" taskblockgroup.py
A TaskBlock that contains other Tasks and executes them.
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
from asyncio import gather, Task, create_task
from typing import Any, NamedTuple

# Third-Party Packages #
from baseobjects import BaseDict

# Local Packages #
from .taskblock import TaskBlock


# Definitions #
# Classes #
class TaskUnit(NamedTuple):
    """A Dataclass that holds a tasks and its execution information.

    Attributes:
        task: The TaskBlock to contain.
        execute_method: The name of the task's method to execute.
        pre_setup: Determines if the setup will be run before the other tasks start execution.
        post_teardown: Determines if the teardown will be run after the other tasks finish execution.
    """
    task: TaskBlock
    execute_method: str
    pre_setup: bool
    post_teardown: bool


class TaskBlockGroup(TaskBlock, BaseDict):
    """A TaskBlock that contains other Tasks and executes them.

    TaskBlock Group contains TaskUnits rather Tasks because each task has some external execution information required to
    execute. The TaskUnit holds both the TaskBlock and its execution information, because it is more consistent to store them
    in the same object rather have separate list which can lead to errors if there are changes to one but not the
    others.

    Class Attributes:
        unit_type: The type which the contained task units are.

    Attributes:
        _execution_order: The order to execute the contained tasks by name.

    Args:
        name: Name of this object.
        tasks: The contained Tasks to execute.
        order: The order of the tasks to be used by name.
        sets_up: Determines if setup will be run.
        tears_down: Determines if teardown will be run.
        is_process: Determines if this task will run in another process.
        s_kwargs: Contains the keyword arguments to be used in the setup method.
        t_kwargs: Contains the keyword arguments to be used in the task method.
        d_kwargs: Contains the keyword arguments to be used in the teardown method.
        *args: Arguments for inheritance.
        init: Determines if this object should be initialized.
        **kwargs: Keyword arguments for inheritance.
    """
    unit_type: type[TaskUnit] = TaskUnit

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        name: str = "",
        tasks: dict[str, TaskBlock] | None = None,
        order: tuple[str, ...] = (),
        sets_up: bool = True,
        tears_down: bool = True,
        is_process: bool = False,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self._execution_order: tuple[str, ...] = ()

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construct #
        if init:
            self.construct(
                name=name,
                tasks=tasks,
                order=order,
                sets_up=sets_up,
                tears_down=tears_down,
                is_process=is_process,
                s_kwargs=s_kwargs,
                t_kwargs=t_kwargs,
                d_kwargs=d_kwargs,
            )

    @property
    def execution_order(self) -> tuple[str, ...]:
        """Gets the order to execute the tasks. If not set, it will return the current order of tasks.

        The set raises an error if the order does not include all tasks.
        """
        if self._execution_order:
            return self._execution_order
        else:
            return tuple(self.tasks.keys())

    @execution_order.setter
    def execution_order(self, value):
        if set(value) == set(self.tasks.keys()):
            self._execution_order = value
        else:
            raise ValueError("The execution order must contain all tasks.")

    @property
    def tasks(self) -> dict[str, TaskUnit]:
        """The contained Tasks to execute."""
        return self.data

    @tasks.setter
    def tasks(self, value: dict[str, TaskUnit]) -> None:
        self.data = value

    # Constructors/Destructors
    def construct(
        self,
        name: str | None = None,
        tasks: dict[str, TaskBlock] | None = None,
        order: tuple[str, ...] | None = None,
        sets_up: bool = True,
        tears_down: bool = True,
        is_process: bool | None = None,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            name: Name of this object.
            tasks: The tasks to execute.
            order: The order of the tasks to be used by name.
            sets_up: Determines if setup will be run.
            tears_down: Determines if teardown will be run.
            is_process: Determines if this task will run in another process.
            s_kwargs: Contains the keyword arguments to be used in the setup method.
            t_kwargs: Contains the keyword arguments to be used in the task method.
            d_kwargs: Contains the keyword arguments to be used in the teardown method.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance
        """
        if tasks is not None:
            self.update(tasks)

        if order is not None:
            self.execution_order = order

        # Construct Parent #
        super().construct(
            name=name,
            sets_up=sets_up,
            tears_down=tears_down,
            is_process=is_process,
            s_kwargs=s_kwargs,
            t_kwargs=t_kwargs,
            d_kwargs=d_kwargs,
            *args,
            **kwargs,
        )

    # State
    def all_process(self) -> bool:
        """Checks if all the contained tasks are processes.

        Returns:
            If all the contained tasks are processes.
        """
        return all((task.is_process for task in self.tasks.values()))

    def any_process(self) -> bool:
        """Checks if any the contained tasks are processes.

        Returns:
            If any the contained tasks are processes.
        """
        return any((task.is_process for task in self.tasks.values()))

    def create_unit(
        self,
        task: TaskBlock,
        execute_method: str = "run",
        pre_setup: bool = False,
        post_teardown: bool = False,
    ) -> TaskUnit:
        """Creates a unit from a given task and its information.

        Args:
            task: The TaskBlock to contain.
            execute_method: The name of the task's method to execute.
            pre_setup: Determines if the setup will be run before the other tasks start execution.
            post_teardown: Determines if the teardown will be run after the other tasks finish execution.
        """
        return self.unit_type(task, execute_method, pre_setup, post_teardown)

    def set_task(
        self,
        name: str,
        task: TaskBlock,
        execute_method: str = "run",
        pre_setup: bool = False,
        post_teardown: bool = False,
    ) -> None:
        """Sets a single unit within this object.

        Args:
            name: The name of the unit.
            task: The TaskBlock to contain.
            execute_method: The name of the task's method to execute.
            pre_setup: Determines if the setup will be run before the other tasks start execution.
            post_teardown: Determines if the teardown will be run after the other tasks finish execution.
        """
        self.data[name] = self.create_unit(task, execute_method, pre_setup, post_teardown)

    def update(self, dict_: Any = None, /, **kwargs: Any) -> None:
        """Updates the contained unit dictionary with the new dictionary.

        Args:
            dict_: The new dictionary to add to the units of this object.
        """
        for name, unit in (dict(dict_) | kwargs).items():
            if isinstance(unit, TaskUnit):
                self.data[name] = unit
            elif isinstance(unit, dict):
                self.data[name] = self.create_unit(**unit)
            else:
                self.data[name] = self.create_unit(unit)

    # Setup
    async def setup(self, *args: Any, **kwargs: Any) -> None:
        """Setup this object and the contained tasks if they are setting up early."""
        # Execute the setup of the tasks if they have pre-setup
        for name in self.execution_order:
            unit = self.data[name]
            if unit.pre_setup:
                await unit.task.setup_async()

    # TaskBlock
    async def task(self, *args: Any, **kwargs: Any) -> None:
        """The main method to execute the contained tasks."""
        async_tasks = []

        # Execute all tasks
        for name in self.execution_order:
            unit = self.data[name]
            awaitable = getattr(unit.task, unit.excute_method)()
            if awaitable is not None:
                async_tasks.append(awaitable)

        await gather(*async_tasks)

    # Teardown
    async def teardown(self, *args: Any, **kwargs: Any) -> None:
        """Teardown this object and the contained tasks if they are tearing down late."""
        # Execute the teardown of the tasks if they have post-teardown
        for name in self.execution_order:
            unit = self.data[name]
            if unit.pre_teardown:
                await unit.task.teardown_async()

    # Joins
    def join_tasks(self, timeout: float | None = None) -> None:
        """Wait until all the tasks terminate.

        Args:
            timeout: The time, in seconds, to wait for termination for each task.
        """
        # Join all tasks
        for name in self.execution_order:
            self.data[name].task.join(timeout)

    async def join_tasks_async(self, timeout: float | None = None, interval: float = 0.0) -> None:
        """Asynchronously wait until all the tasks terminate.

        Args:
            timeout: The time, in seconds, to wait for termination.
            interval: The time, in seconds, between each join check.
        """
        async_tasks = []
        for name in self.execution_order:
            async_tasks.append(self.tasks[name].task.join_async_task(timeout=timeout, interval=interval))

        await gather(*async_tasks)

    def join_tasks_async_task(self, timeout: float | None = None, interval: float = 0.0) -> Task:
        """Creates waiting for all tasks to terminate as an asyncio task.

        Args:
            timeout: The time, in seconds, to wait for termination.
            interval: The time, in seconds, between each join check.
        """
        return create_task(self.join_tasks_async(timeout=timeout, interval=interval))

    def join_all(self, timeout: float | None = None) -> None:
        """Wait until this object and all the tasks terminate.

        Args:
            timeout: The time, in seconds, to wait for termination for each task.
        """
        self.join(timeout)
        # Join all tasks
        for name in self.execution_order:
            self.tasks[name].task.join(timeout)

    async def join_all_async(self, timeout: float | None = None, interval: float = 0.0) -> None:
        """Asynchronously wait until this object and all the tasks terminate.

        Args:
            timeout: The time, in seconds, to wait for termination.
            interval: The time, in seconds, between each join check.
        """
        async_tasks = [self.join_async_task(timeout=timeout, interval=interval)]
        for name in self.execution_order:
            async_tasks.append(self.tasks[name].task.join_async_task(timeout=timeout, interval=interval))

        await gather(*async_tasks)

    def join_all_async_task(self, timeout: float | None = None, interval: float = 0.0) -> Task:
        """Creates waiting for this object and all tasks to terminate as an asyncio task.

        Args:
            timeout: The time, in seconds, to wait for termination.
            interval: The time, in seconds, between each join check.
        """
        return create_task(self.join_all_async(timeout=timeout, interval=interval))

    def stop_tasks(self) -> None:
        """Calls the stop method of all tasks."""
        for name in self.execution_order:
            self.tasks[name].task.stop()

    def stop(self) -> None:
        """Calls the stop method of all tasks."""
        super().stop()
        self.stop_tasks()

    def terminate(self) -> None:
        """Terminates the current running task, be unsafe for inputs and outputs."""
        super().terminate()

        # Terminate all tasks
        for name in self.execution_order:
            self.tasks[name].task.terminate()
