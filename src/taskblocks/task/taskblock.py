""" taskblock.py
An abstract processing class whose methods can be overwritten to define its functionality.
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
from asyncio import run, Future, Task, create_task
from asyncio.events import AbstractEventLoop, _get_running_loop
from typing import Any
from warnings import warn

# Third-Party Packages #
from baseobjects.functions import MethodMultiplexObject, MethodMultiplexer

# Local Packages #
from ..io import IOManager, AsyncEvent
from ..process import ProcessProxy


# Definitions #
# Classes #
class TaskBlock(MethodMultiplexObject):
    """An abstract processing class whose methods can be overwritten to define its functionality.

    Attributes:
        name: The name of this object.
        _is_async: Determines if this object will be asynchronous.
        _is_process: Determines if this object will run in a separate process.
        sets_up: Determines if setup will run.
        tears_down: Determines if teardown will run.
        loop_event: The Event used to stop the task loop.
        _alive_event: The Event that determines if alive.

        setup_kwargs: Contains the keyword arguments to be used in the setup method.
        task_kwargs: Contains the keyword arguments to be used in the task method.
        teardown_kwargs: Contains the keyword arguments to be used in the closure method.

        inputs: A handler that contains all inputs for this object.
        outputs: A handler that contains all outputs for this object.
        futures: Futures to await for. Runs after teardown.

        _setup: The method to call when this object executes setup.
        _task: The method to call when this object executes the task.
        _teardown: The method to call when this object executes closure.

    Args:
        name: Name of this object.
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
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        name: str = "",
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
        self.name: str = ""
        self._is_async: bool = True
        self._is_process: bool = False
        self.sets_up: bool = True
        self.tears_down: bool = True
        self.loop_event: AsyncEvent = AsyncEvent()
        self._alive_event: AsyncEvent = AsyncEvent()

        self.setup_kwargs: dict[str, Any] = {}
        self.task_kwargs: dict[str, Any] = {}
        self.teardown_kwargs: dict[str, Any] = {}

        self.inputs: IOManager = IOManager()
        self.outputs: IOManager = IOManager()
        self.futures: list[Future] = []

        self.process: ProcessProxy | None = ProcessProxy()
        self._daemon: bool | None = None

        self._setup: MethodMultiplexer = MethodMultiplexer(instance=self, select="setup")
        self._task: MethodMultiplexer = MethodMultiplexer(instance=self, select="task")
        self._teardown: MethodMultiplexer = MethodMultiplexer(instance=self, select="teardown")

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construct #
        if init:
            self.construct(
                name=name,
                sets_up=sets_up,
                tears_down=tears_down,
                is_process=is_process,
                s_kwargs=s_kwargs,
                t_kwargs=t_kwargs,
                d_kwargs=d_kwargs,
            )

    @property
    def is_process(self) -> bool:
        """bool: If this object will run in a separate process. It will detect if it is in a process while running.

        When set it will raise an error if the TaskBlock is running.
        """
        if self.is_alive():
            return self.process is not None and self.process.is_alive()
        else:
            return self._is_process

    @is_process.setter
    def is_process(self, value: bool) -> None:
        if self.is_alive():
            raise ValueError("Cannot set process while task is alive.")
        else:
            self._is_process = value

    @property
    def async_event_loop(self) -> AbstractEventLoop | None:
        """The async event loop if it is running otherwise None."""
        return _get_running_loop()

    # Pickling
    def __getstate__(self) -> dict[str, Any]:
        """Creates a dictionary of attributes which can be used to rebuild this object.

        Returns:
            dict: A dictionary of this object's attributes.
        """
        out_dict = self.__dict__.copy()
        del out_dict["process"]
        return out_dict

    def __setstate__(self, in_dict: dict[str, Any]) -> None:
        """Builds this object based on a dictionary of corresponding attributes.

        Args:
            in_dict: The attributes to build this object from.
        """
        in_dict["process"] = ProcessProxy()
        self.__dict__ = in_dict

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        name: str | None = None,
        sets_up: bool | None = None,
        tears_down: bool | None = None,
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
            sets_up: Determines if setup will be run.
            tears_down: Determines if teardown will be run.
            is_process: Determines if this task will run in another process.
            s_kwargs: Contains the keyword arguments to be used in the setup method.
            t_kwargs: Contains the keyword arguments to be used in the task method.
            d_kwargs: Contains the keyword arguments to be used in the teardown method.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if name is not None:
            self.name = name

        if sets_up is not None:
            self.sets_up = sets_up

        if tears_down is not None:
            self.tears_down = tears_down

        if is_process is not None:
            self.is_process = is_process

        if s_kwargs is not None:
            self.setup_kwargs = s_kwargs

        if t_kwargs is not None:
            self.task_kwargs = t_kwargs

        if d_kwargs is not None:
            self.teardown_kwargs = d_kwargs

        self.construct_io()

        # Construct Parent #
        super().construct(*args, **kwargs)

    # State Methods
    def is_alive(self) -> bool:
        """Checks if this object is currently running.

        Returns:
            bool: If this object is currently running.
        """
        return self._alive_event.is_set()

    def is_processing(self) -> bool:
        """Checks if this object is currently running in a process.

        Returns:
            bool: If this object is currently running in a process.
        """
        try:
            return self.process.is_alive()
        except AttributeError:
            return False

    # IO
    def construct_io(self) -> None:
        """Abstract method that constructs the io for this object."""
        pass

    def link_input(self, *args: Any, **kwargs: Any) -> None:
        """Abstract method that gives a place to the inputs to other objects."""
        pass

    def link_output(self, *args: Any, **kwargs: Any) -> None:
        """Abstract method that gives a place to the outputs to other objects."""
        pass

    def link_io(self, *args: Any, **kwargs: Any) -> None:
        """Abstract method that gives a place to the io to other objects."""
        pass

    # Process
    def construct_process(self) -> None:
        """Creates a separate process for this task."""
        self.process = ProcessProxy(name=self.name, daemon=self._daemon)

    # Setup
    def setup(self, *args: Any, **kwargs: Any) -> None:
        """The method to run before executing task."""
        pass

    async def setup_async(self, *args: Any, **kwargs: Any) -> None:
        """Asynchronously runs the setup."""
        if self._setup.is_coroutine:
            await self._setup(*args, **(self.setup_kwargs | kwargs))
        else:
            self._setup(*args, **(self.setup_kwargs | kwargs))

    # TaskBlock
    def task(self, *args: Any, **kwargs: Any) -> None:
        """The main method to execute."""
        pass

    async def task_async(self, *args: Any, **kwargs: Any) -> None:
        """The main async method to execute."""
        self._task(*args, **kwargs)

    async def loop_task(self, *args: Any, **kwargs: Any) -> None:
        """An async loop that executes the _task consecutively until an event stops it."""
        if self._task.is_coroutine:
            while self.loop_event.is_set():
                try:
                    await create_task(self._task(*args, **kwargs))
                except InterruptedError as e:
                    warn("TaskBlock interrupted, if intentional, handle in the task.")
        else:
            while self.loop_event.is_set():
                try:
                    await create_task(self.task_async(*args, **kwargs))
                except InterruptedError:
                    warn("TaskBlock interrupted, if intentional, handle in the task.")

    # Teardown
    def teardown(self, *args: Any, **kwargs: Any) -> None:
        """The method to run after executing task."""
        pass

    async def teardown_async(self, *args: Any, **kwargs: Any) -> None:
        """Asynchronously runs the teardown."""
        if self._teardown.is_coroutine:
            await self._teardown(*args, **(self.teardown_kwargs | kwargs))
        else:
            self._teardown(*args, **(self.teardown_kwargs | kwargs))
    
    # Run TaskBlock Once
    async def _run(
        self,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Executes a single run of the task.

        Args:
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        # Flag On
        self._alive_event.set()

        # Optionally Setup
        if self.sets_up:
            await self.setup_async(**(s_kwargs or {}))

        # Run TaskBlock
        if self._task.is_coroutine:
            await self._task(**(self.task_kwargs | (t_kwargs or {})))
        else:
            await self.task_async(**(self.task_kwargs | (t_kwargs or {})))

        # Optionally Teardown
        if self.tears_down:
            await self.teardown_async(**(d_kwargs or {}))

        # Wait for any remaining Futures
        for future in self.futures:
            await future

        # Flag Off
        self._alive_event.clear()

    def _run_async_loop(
        self,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Executes a single run of the task.

        Args:
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        run(self._run(s_kwargs=s_kwargs, t_kwargs=t_kwargs, d_kwargs=d_kwargs))

    async def run_async(
        self,
        is_process: bool | None = None,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Executes a single async run of the task and delegates to another process is selected.

        Args:
            is_process: Determines if this object should run in a separate process.
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        # Raise Error if the task is already running.
        if self._alive_event.is_set():
            raise RuntimeError(f"{self} task is already running.")

        # Set to Alive
        self._alive_event.set()

        # Set separate process
        if is_process is not None:
            self._is_process = is_process

        # Use Correct Context
        if self._is_process:
            self.process.target = self._run_async_loop
            self.process.kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "d_kwargs": d_kwargs}
            self.process.start()
        else:
            await self._run(s_kwargs, t_kwargs, d_kwargs)

    def run(
        self,
        is_process: bool | None = None,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> Task | None:
        """Executes a single run of the task and delegates to another process is selected.

        Args:
            is_process: Determines if this object should run in a separate process.
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        # Raise Error if the task is already running.
        if self._alive_event.is_set():
            raise RuntimeError(f"{self} task is already running.")

        # Set to Alive
        self._alive_event.set()

        # Set separate process
        if is_process is not None:
            self._is_process = is_process

        # Use Correct Context
        if self._is_process:
            self.process.target = self._run_async_loop
            self.process.kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "d_kwargs": d_kwargs}
            self.process.start()
            return
        elif self.async_event_loop is not None:
            return create_task(self._run(s_kwargs, t_kwargs, d_kwargs))
        else:
            self._run_async_loop(s_kwargs, t_kwargs, d_kwargs)
            return

    # Start TaskBlock Loop
    async def _start(
        self,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Starts the continuous execution of the task.

        Args:
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        # Flag On
        self._alive_event.set()
        self.loop_event.set()

        # Optionally Setup
        if self.sets_up:
            await self.setup_async(**(s_kwargs or {}))

        # Loop TaskBlock
        await self.loop_task(**(self.task_kwargs | (t_kwargs or {})))

        # Optionally Teardown
        if self.tears_down:
            await self.teardown_async(**(d_kwargs or {}))

        # Wait for any remaining Futures
        for future in self.futures:
            await future

        # Flag Off
        self._alive_event.clear()

    def _start_async_loop(
        self,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Starts the continuous execution of the task in an async run.

        Args:
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        run(self._start(s_kwargs=s_kwargs, t_kwargs=t_kwargs, d_kwargs=d_kwargs))

    async def start_async(
        self,
        is_process: bool | None = None,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Starts the async continuous execution of the task and delegates to another process is selected.

        Args:
            is_process: Determines if this object should start in a separate process.
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        # Raise Error if the task is already running.
        if self._alive_event.is_set():
            raise RuntimeError(f"{self} task is already running.")

        # Set to Alive
        self._alive_event.set()

        # Set separate process
        if is_process is not None:
            self._is_process = is_process

        # Use Correct Context
        if self._is_process:
            self.process.target = self._start_async_loop
            self.process.kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "d_kwargs": d_kwargs}
            self.process.start()
        else:
            await self._start(s_kwargs, t_kwargs, d_kwargs)

    def start(
        self,
        is_process: bool | None = None,
        s_kwargs: dict[str, Any] | None = None,
        t_kwargs: dict[str, Any] | None = None,
        d_kwargs: dict[str, Any] | None = None,
    ) -> Task | None:
        """Starts the continuous execution of the task and delegates to another process is selected.

        Args:
            is_process: Determines if this object should start in a separate process.
            s_kwargs: The keyword arguments for task setup.
            t_kwargs: The keyword arguments for the task.
            d_kwargs: The keyword arguments for task teardown.
        """
        # Raise Error if the task is already running.
        if self._alive_event.is_set():
            raise RuntimeError(f"{self} task is already running.")

        # Set to Alive
        self._alive_event.set()

        # Set separate process
        if is_process is not None:
            self._is_process = is_process

        # Use Correct Context
        if self._is_process:
            self.process.target = self._start_async_loop
            self.process.kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "d_kwargs": d_kwargs}
            self.process.start()
            return
        elif self.async_event_loop is not None:
            return create_task(self._start(s_kwargs, t_kwargs, d_kwargs))
        else:
            self._start_async_loop(s_kwargs, t_kwargs, d_kwargs)
            return

    # Joins
    def join(self, timeout: float | None = None) -> None:
        """Wait until this object terminates.

        Args:
            timeout: The time, in seconds, to wait for termination.
        """
        if self.is_process:
            self.process.join(timeout=timeout)
        else:
            self._alive_event.hold(timeout=timeout)

    async def join_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> None:
        """Asynchronously wait until this object terminates.

        Args:
            timeout: The time, in seconds, to wait for termination.
            interval: The time, in seconds, between each join check.
        """
        if self.is_process:
            await self.process.join_async(timeout=timeout, interval=interval)
        else:
            await self._alive_event.hold_async(timeout=timeout, interval=interval)

    def join_async_task(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Task:
        """Creates waiting for this object to terminate as an asyncio task.

        Args:
            timeout: The time, in seconds, to wait for termination.
            interval: The time, in seconds, between each join check.
        """
        return create_task(self.join_async(timeout=timeout, interval=interval))

    def stop(self) -> None:
        """Abstract method that should stop this task."""
        self.loop_event.clear()

    def terminate(self) -> None:
        """Terminates the current running task, be unsafe for inputs and outputs."""
        self.loop_event.clear()
        self.inputs.interrupt_all()
        self.outputs.interrupt_all()
