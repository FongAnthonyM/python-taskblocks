#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processingblocks.py
In Python multiprocessing, separate processes do not share scopes but create copies. This means if an object is passed
from one process to another and the new process edits that object, the original process is not going to see the
change. Furthermore, when an object is passed to another process it is pickled and rebuilt in the other process which is
handled by multiprocessing's Pipes and Queues. Building an application that uses multiprocessing can be difficult
because the developer needs to keep track of what is in each processes' scope. The aim of this package is to provide a
block paradigm framework to separate components of an application into self contained blocks that can run in separate
processes.

Blocks are self contained objects

"""
__author__ = "Anthony Fong"
__copyright__ = "Copyright 2021, Anthony Fong"
__credits__ = ["Anthony Fong"]
__license__ = ""
__version__ = "0.1.0"
__maintainer__ = "Anthony Fong"
__email__ = ""
__status__ = "Prototype"

# Default Libraries #
import asyncio
import collections
import dataclasses
import multiprocessing
import multiprocessing.connection
from multiprocessing import Process, Pool, Lock, Event, Queue, Pipe
import warnings
import time
import typing

# Downloaded Libraries #
import advancedlogging
from advancedlogging import AdvancedLogger, ObjectWithLogging
from dynamicwrapper import DynamicWrapper

# Local Libraries #
from . import processingio


# Definitions #
# Classes #
# Tasks #
class Task(ObjectWithLogging):
    """The most basic block unit.

    A Task is the basic block unit and is a self contained

    Class Attributes:
        class_loggers (dict): The default loggers to include in every object of this class.

    Attributes:
        loggers (dict): A collection of loggers used by this object. The keys are the names of the different loggers.
        name (str): The name of this object.
        async_loop: The event loop to assign the async methods to.
        allow_setup (bool): Determines if setup will be run.
        allow_closure (bool): Determines if closure will be run.
        alive_event (:obj:``Event): The Event that determines if alive.
        terminate_event (:obj:`Event`): The Event used to stop the task loop.
        events (dict): Holds the Events for this object.
        locks (dict): Holds the Locks for this object.

        setup_kwargs (dict): Contains the keyword arguments to be used in the setup method.
        task_kwargs (dict): Contains the keyword arguments to be used in the task method.
        closure_kwargs (dict): Contains the keyword arguments to be used in the closure method.

        inputs: A handler that contains all of the inputs for this object.
        outputs: A handler that contains all of the outputs for this object.

        _execute_setup: The function to call when this object executes setup.
        _execute_task: The function to call when this object executes the task.
        _execute_task_loop: The function to call when this object executes task looping.
        _execute_closure: The function to call when this object executes closure.

    Args:
        name (str, optional): Name of this object.
        allow_setup (bool, optional): Determines if setup will be run.
        allow_closure (bool, optional): Determines if closure will be run.
        s_kwargs (dict, optional): Contains the keyword arguments to be used in the setup method.
        t_kwargs (dict, optional): Contains the keyword arguments to be used in the task method.
        c_kwargs (dict, optional): Contains the keyword arguments to be used in the closure method.
        init (bool, optional): Determines if this object should be initialized.
    """
    class_loggers = {"task_root": AdvancedLogger("task_root")}

    # Construction/Destruction
    def __init__(self, name=None, allow_setup=True, allow_closure=True,
                 s_kwargs={}, t_kwargs={}, c_kwargs={}, init=True):
        super().__init__()
        self.name = ""
        self.async_loop = asyncio.get_event_loop()
        self.allow_setup = True
        self.allow_closure = True
        self.alive_event = Event()
        self.terminate_event = Event()
        self.events = {}
        self.locks = {}

        self.setup_kwargs = {}
        self.task_kwargs = {}
        self.closure_kwargs = {}

        self.inputs = None
        self.outputs = None

        self._execute_setup = self.setup
        self._execute_task = self.task
        self._execute_task_loop = self.task_loop
        self._execute_closure = self.closure

        if init:
            self.construct(name, allow_setup, allow_closure, s_kwargs, t_kwargs, c_kwargs)

    # Pickling
    def __getstate__(self):
        """Creates a dictionary of attributes which can be used to rebuild this object.

        Returns:
            dict: A dictionary of this object's attributes.
        """
        out_dict = self.__dict__.copy()
        del out_dict["async_loop"]
        return out_dict

    def __setstate__(self, in_dict):
        """Builds this object based on a dictionary of corresponding attributes.

        Args:
            in_dict (dict): The attributes to build this object from.
        """
        in_dict["async_loop"] = asyncio.get_event_loop()
        self.__dict__ = in_dict

    # Constructors/Destructors
    def construct(self, name=None, allow_setup=True, allow_closure=True, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Constructs this object.

        Args:
            name (str, optional): Name of this object.
            allow_setup (bool, optional): Determines if setup will be run.
            allow_closure (bool, optional): Determines if closure will be run.
            s_kwargs (dict, optional): Contains the keyword arguments to be used in the setup method.
            t_kwargs (dict, optional): Contains the keyword arguments to be used in the task method.
            c_kwargs (dict, optional): Contains the keyword arguments to be used in the closure method.
        """
        self.name = name
        self.setup_kwargs = s_kwargs
        self.task_kwargs = t_kwargs
        self.closure_kwargs = c_kwargs
        self.allow_setup = allow_setup
        self.allow_closure = allow_closure

        self.construct_io()

    # State Methods
    def is_alive(self):
        """Checks if this object is currently running.

        Returns:
            bool: If this object is currently running
        """
        return self.alive_event.is_set()

    def is_async(self):
        """Checks if this object is using async.

        Returns:
            bool: If this object is using async.
        """
        if asyncio.iscoroutinefunction(self._execute_setup) or asyncio.iscoroutinefunction(self._execute_closure) or \
           asyncio.iscoroutinefunction(self._execute_task):
            return True
        else:
            return False

    # IO
    def construct_io(self):
        """Constructs the io for this object."""
        self.inputs = processingio.InputsHandler(name=self.name)
        self.outputs = processingio.OutputsHandler(name=self.name)

        self.inputs.create_event("SelfStop")

        self.create_io()

    def create_io(self):
        """Creates the individual io elements for this object."""
        pass

    # Multiprocess Event
    def create_event(self, name):
        """Creates a new Event object and stores.

        Args:
            name (str): The name of the Event.

        Returns:
            (obj:`Event`): The Event
        """
        self.events[name] = Event()
        return self.events[name]

    def set_event(self, name, event):
        """Adds an Event to this object.

        Args:
            name (str): The name of the Event.
            event (:obj:`Event`): The Event object to add.
        """
        self.events[name] = event

    # Multiprocess Lock
    def create_lock(self, name):
        """Creates a new Lock object and stores.

        Args:
            name (str): The name of the Lock.

        Returns:
            (obj:`Event`): The Lock
        """
        self.locks[name] = Lock()
        return self.locks[name]

    def set_lock(self, name, lock):
        """Adds an Lock to this object.

        Args:
            name (str): The name of the Lock.
            lock (:obj:`Lock`): The lock object to add.
        """
        self.locks[name] = lock

    # Setup
    def setup(self, **kwargs):
        """The method to run before executing task."""
        self.trace_log("task_root", "setup", "setup method not overridden", name=self.name, level="DEBUG")

    # Task
    def task(self, **kwargs):
        """The main method to execute."""
        self.trace_log("task_root", "task", "task method not overridden", name=self.name, level="DEBUG")

    async def task_async(self, **kwargs):
        """The main async method to execute."""
        self.trace_log("task_root", "task_async", "task_async method not overridden running task",
                       name=self.name, level="DEBUG")
        self.task(**kwargs)

    def task_loop(self, **kwargs):
        """A loop that executes the _execute_task consecutively until an event stops it."""
        while not self.stop_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                self._execute_task(**kwargs)
            else:
                self.stop_event.set()

    async def task_loop_async(self, **kwargs):
        """An async loop that executes the _execute_task consecutively until an event stops it."""
        while not self.terminate_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                await asyncio.create_task(self._execute_task(**kwargs))
            else:
                self.terminate_event.set()

    # Closure
    def closure(self, **kwargs):
        """The method to run after executing task."""
        self.trace_log("task_root", "closure", "closure method not overridden", name=self.name, level="DEBUG")

    # Getters/Setters for Execution Functions
    def get_setup(self):
        """Gets the setup function to run.

        Returns:
            The setup function
        """
        return self._execute_setup

    def set_setup(self, func, kwargs={}):
        """Sets the setup function to run.

        Args:
            func: The function to run on setup.
            kwargs: The key word arguments to run on setup.
        """
        if kwargs:
            self.setup_kwargs = kwargs
        self._execute_setup = func

    def get_task(self):
        """Gets the task function to run.

        Returns:
            The task function
        """
        return self._execute_task

    def set_task(self, func, kwargs={}):
        """Sets the task function to run.

        Args:
            func: The function which will be the task.
            kwargs: The key word arguments to run the task with.
        """
        if kwargs:
            self.task_kwargs = kwargs
        self._execute_task = func

    def get_task_loop(self):
        """Gets the task loop function to run.

        Returns:
            The task loop function
        """
        return self._execute_task_loop

    def set_task_loop(self, func):
        self._execute_task_loop = func

    def get_closure(self):
        """Gets the closure function to run.

        Returns:
            The closure function
        """
        return self._execute_closure

    def set_closure(self, func, kwargs={}):
        """Sets the closure function to run.

        Args:
            func: The function to run on closure.
            kwargs: The key word arguments to run on closure.
        """
        if kwargs:
            self.closure_kwargs = kwargs
        self._execute_closure = func

    # Preparation
    def prepare_task(self):
        """Changes the execute task to the correct type if it is wrong."""
        if self._execute_task == self.task:
            if self.is_async():
                self._execute_task = self.task_async
        elif self._execute_task == self.task_async:
            if not self.is_async():
                self._execute_task = self.task

    def prepare_task_loop(self):
        """Changes the execute task loop to the correct type if it is wrong."""
        # Change the task loop to async if the task is async
        if self._execute_task_loop == self.task_loop:
            if asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop_async
        # Change the task loop to normal if the task is normal
        elif self._execute_task_loop == self.task_loop_async:
            if not asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop

    # Section Execution
    def execute_setup(self, func_name="execute_setup", allow_setup=None, **kwargs):
        """Executes the _execute_setup function.

        Args:
            func_name (str): The name of the function this method is being called from for logging.
            allow_setup (bool): Determines if setup should be run.
            **kwargs: The keyword arguments for _execute_setup function.
        """
        if allow_setup is None:
            allow_setup = self.allow_setup
        if kwargs:
            self.setup_kwargs = kwargs

        if allow_setup:
            self.trace_log("task_root", func_name, "running setup", name=self.name, level="DEBUG")
            self._execute_setup(**self.setup_kwargs)
        else:
            self.trace_log("task_root", func_name, "skipping setup", name=self.name, level="DEBUG")

    def execute_closure(self, func_name="execute_closure", allow_closure=None, **kwargs):
        """Executes the _execute_closure function.

        Args:
            func_name (str): The name of the function this method is being called from for logging.
            allow_closure (bool): Determines if closure should be run.
            **kwargs: The keyword arguments for _execute_closure function.
        """
        if allow_closure is not None:
            allow_closure = self.allow_closure
        if kwargs:
            self.closure_kwargs = kwargs

        if allow_closure:
            self.trace_log("task_root", func_name, "running closure", name=self.name, level="DEBUG")
            self._execute_closure(**self.closure_kwargs)
        else:
            self.trace_log("task_root", func_name, "skipping closure", name=self.name, level="DEBUG")

    async def execute_setup_async(self, func_name="execute_setup_async", allow_setup=None, **kwargs):
        """Executes the _execute_setup function will async support.

        Args:
            func_name (str): The name of the function this method is being called from for logging.
            allow_setup (bool): Determines if setup should be run.
            **kwargs: The keyword arguments for _execute_setup function.
        """
        if allow_setup is not None:
            allow_setup = self.allow_setup
        if kwargs:
            self.setup_kwargs = kwargs

        if allow_setup:
            if asyncio.iscoroutinefunction(self._execute_setup):
                self.trace_log("task_root", func_name, "running async setup", name=self.name, level="DEBUG")
                await self._execute_setup(**self.setup_kwargs)
            else:
                self.trace_log("task_root", func_name, "running setup", name=self.name, level="DEBUG")
                self._execute_setup(**self.setup_kwargs)
        else:
            self.trace_log("task_root", func_name, "skipping setup", name=self.name, level="DEBUG")

    async def execute_closure_async(self, func_name="execute_closure_async", allow_closure=None, **kwargs):
        """Executes the _execute_setup function will async support.

        Args:
            func_name (str): The name of the function this method is being called from for logging.
            allow_closure (bool): Determines if closure should be run.
            **kwargs: The keyword arguments for _execute_setup function.
        """
        if allow_closure is not None:
            allow_closure = self.allow_closure
        if kwargs:
            self.closure_kwargs = kwargs

        if allow_closure:
            if asyncio.iscoroutinefunction(self._execute_closure):
                self.trace_log("task_root", func_name, "running async closure", name=self.name, level="DEBUG")
                await self._execute_closure(**self.closure_kwargs)
            else:
                self.trace_log("task_root", func_name, "running closure", name=self.name, level="DEBUG")
                self._execute_closure(**self.closure_kwargs)
        else:
            self.trace_log("task_root", func_name, "skipping closure", name=self.name, level="DEBUG")

    # Normal Execution
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Executes a single run of the task with setup and closure.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Optionally run Setup
        self.execute_setup(func_name="run_normal", **s_kwargs)

        # Run Task Loop
        if t_kwargs:
            self.task_kwargs = t_kwargs
        self.trace_log("task_root", "run_normal", "running task", name=self.name, level="DEBUG")
        self._execute_task(**self.task_kwargs)

        # Optionally run Closure
        self.execute_closure(func_name="run_normal", **c_kwargs)

        # Flag Off
        self.alive_event.clear()

    def start_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Starts the continuous execution of the task with setup and closure.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Set the task loop if incorrect
        self.prepare_task_loop()

        # Optionally run Setup
        self.execute_setup(func_name="start_normal", **s_kwargs)

        # Run Task Loop
        if t_kwargs:
            self.task_kwargs = t_kwargs
        self.trace_log("task_root", "start_normal", "running task", name=self.name, level="DEBUG")
        self._execute_task_loop(**self.task_kwargs)

        # Optionally run Closure
        self.execute_closure(func_name="start_normal", **c_kwargs)

        # Flag Off
        self.alive_event.clear()

    # Async Execution
    async def run_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Executes a single run of the async task with setup and closure.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Optionally run Setup
        await self.execute_setup_async(func_name="run_coro", **s_kwargs)

        # Run Task
        if t_kwargs:
            self.task_kwargs = t_kwargs
        if asyncio.iscoroutinefunction(self._execute_task):
            self.trace_log("task_root", "run_coro", "running async task", name=self.name, level="DEBUG")
            await self._execute_task(**self.task_kwargs)
        else:
            self.trace_log("task_root", "run_coro", "running task", name=self.name, level="DEBUG")
            self._execute_task(**self.task_kwargs)

        # Optionally run Closure
        await self.execute_closure_async(func_name="run_coro", **c_kwargs)

        # Flag Off
        self.alive_event.clear()

    async def start_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Starts the continuous async execution of the task with setup and closure.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Set the task loop if incorrect
        self.prepare_task_loop()

        # Optionally run Setup
        await self.execute_setup_async(func_name="start_coro", **s_kwargs)

        # Run Task Loop
        if t_kwargs:
            self.task_kwargs = t_kwargs
        if asyncio.iscoroutinefunction(self._execute_task):
            self.trace_log("task_root", "start_coro", "running async task", name=self.name, level="DEBUG")
            await self._execute_task_loop(**self.task_kwargs)
        else:
            self.trace_log("task_root", "start_coro", "running task", name=self.name, level="DEBUG")
            self._execute_task_loop(**self.task_kwargs)

        # Optionally run Closure
        await self.execute_closure_async(func_name="start_coro", **c_kwargs)

        # Flag Off
        self.alive_event.clear()

    # Joins
    def join_normal(self, timeout=None):
        """Wait until this object terminates.

        Args:
            timeout (float): The time in seconds to wait for termination.
        """
        start_time = time.perf_counter()
        while self.alive_event.is_set():
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

    async def join_async(self, timeout=None, interval=0.0):
        """Asynchronously wait until this object terminates.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        start_time = time.perf_counter()
        while self.alive_event.is_set():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

    # Full Execution
    def run(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Executes a single run of the task and determines if the async version should be run.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        self.alive_event.set()

        # Set the task if incorrect
        self.prepare_task()

        # Use Correct Context
        if self.is_async():
            asyncio.run(self.run_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.run_normal(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Creates a single execution of this task as an asyncio task.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        self.alive_event.set()

        # Set the task if incorrect
        self.prepare_task()

        # Create Async Event
        return asyncio.create_task(self.run_coro(s_kwargs, t_kwargs, c_kwargs))

    def start(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Starts the execution of multiple runs of the task and determines if the async version should be run.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        self.alive_event.set()

        # Set the task if incorrect
        self.prepare_task()

        # Use Correct Context
        if self.is_async():
            asyncio.run(self.start_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.start_normal(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Creates the continuous execution of this task as an asyncio task.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Flag On
        self.alive_event.set()

        # Set the task if incorrect
        self.prepare_task()

        # Create Async Event
        return asyncio.create_task(self.start_coro(s_kwargs, t_kwargs, c_kwargs))

    def join(self, timeout=None, interval=0.0):
        """Wait until this object terminates and determines if the async version should be run.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        # Use Correct Context
        if self.is_async():
            asyncio.run(self.join_async(timeout, interval))
        else:
            self.join_normal(timeout)

    def join_async_task(self, timeout=None, interval=0.0):
        """Creates waiting for this object to terminate as an asyncio task.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        return asyncio.create_task(self.join_async(timeout, interval))

    def terminate(self, join=False, timeout=None, interval=0.0):
        """Flags the task loop and task to stop running.

        Args:
            join (bool): Determines if terminate will wait for the object to join.
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        self.trace_log("task_root", "terminate", "terminate was called", name=self.name, level="DEBUG")
        self.terminate_event.set()
        self.inputs.stop_all()
        self.outputs.stop_all()
        if join:
            self.join(timeout, interval)

    def reset(self):
        """Resets the stop flag to allow this task to run again."""
        self.terminate_event.clear()


@dataclasses.dataclass
class MultiTaskUnit:
    """A Dataclass that hold information about task units for """
    name: str
    obj: typing.Any
    execute_type: str
    pre_setup: bool
    post_closure: bool


class MultiUnitTask(Task):
    # Construction/Destruction
    def __init__(self, name=None, units={}, order=(),
                 allow_setup=True, allow_closure=True, s_kwargs={}, t_kwargs={}, c_kwargs={}, init=True):
        # Run Parent __init__ but only construct in child
        super().__init__(init=False)

        # Attributes
        self._execution_order = ()

        self.units = {}

        # Optionally Construct this object
        if init:
            self.construct(units=units, order=order, name=name, allow_setup=allow_setup, allow_closure=allow_closure,
                           s_kwargs=s_kwargs, t_kwargs=t_kwargs, c_kwargs=c_kwargs)

    @property
    def execution_order(self):
        if self._execution_order:
            return self._execution_order
        else:
            return self.units.keys()

    @execution_order.setter
    def execution_order(self, value):
        if len(value) == len(self.units):
            self._execution_order = value
        else:
            warnings.warn()

    # Container Methods
    def __len__(self):
        return len(self.units)

    def __getitem__(self, key):
        if key in self.units:
            return self.units[key]
        raise KeyError(key)

    def __delitem__(self, key):
        del self.units[key]

    # Constructors/Destructors
    def construct(self, units={}, order=(), **kwargs):
        super().construct(**kwargs)
        if units:
            self.update(units=units)
        if order:
            self.execution_order = order

    # State
    def all_async(self):
        for unit in self.units.values():
            if not unit.obj.is_async():
                return False
        return True

    def any_async(self):
        for unit in self.units.values():
            if unit.obj.is_async():
                return True
        return False

    # Container Methods
    def keys(self):
        return self.units.keys()

    def values(self):
        return self.units.values()

    def items(self):
        return self.units.items()

    def set_unit(self, name, obj, execution_type="run", pre_setup=False, post_closure=False):
        self.units[name] = MultiTaskUnit(name, obj, execution_type, pre_setup, post_closure)

    def update(self, units):
        for name, unit in units.items():
            if isinstance(unit, MultiTaskUnit):
                self.units[name] = unit
            elif isinstance(unit, dict):
                if "name" not in unit:
                    unit["name"] = name
                self.units[name] = MultiTaskUnit(**unit)

    def pop(self, name):
        return self.units.pop(name)

    def clear(self):
        self.units.clear()

    # Setup
    def setup(self, **kwargs):
        # Execute the setup of the units if they have pre-setup
        for name in self.execution_order:
            unit = self.units[name]
            if unit.pre_setup:
                unit.obj.execute_setup(allow_setup=True)

    # Task
    def task(self, **kwargs):
        # Execute all of the units
        for name in self.execution_order:
            unit = self.units[name]
            if unit.excute_type == "start":
                unit.obj.start()
            else:
                unit.obj.run()

    async def task_async(self):
        async_tasks = []
        sequential_executions = []

        # Create objects to execute
        for name in self.execution_order:
            unit = self.units[name]
            obj = unit.obj
            if unit.excute_type == "start":
                if obj.is_async():
                    async_tasks.append(obj.start_async_task())
                else:
                    sequential_executions.append(obj.start)
            else:
                if obj.is_async():
                    async_tasks.append(obj.run_async_task())
                else:
                    sequential_executions.append(obj.run)

        # Execute all of the units
        for execute in sequential_executions:
            execute()

        for task in async_tasks:
            await task

    # Closure
    def closure(self, **kwargs):
        # Execute the setup of the units if they have post-closure
        for name in self.execution_order:
            unit = self.units[name]
            if unit.post_closure:
                unit.obj.execute_closure(allow_closure=True)

    # Preparation
    def prepare_task(self):
        if self._execute_task == self.task:
            if self.any_async():
                self._execute_task = self.task_async
        elif self._execute_task == self.task_async:
            if not self.any_async():
                self._execute_task = self.task

    # Joins
    def join_units_normal(self, timeout=None):
        """Wait until all the units terminates.

        Args:
            timeout (float): The time in seconds to wait for termination for each unit.
        """
        # Join all units
        for name in self.execution_order:
            self.units[name].obj.join_normal(timeout)

    def join_all_normal(self, timeout=None):
        """Wait until this object and all the units terminate.

        Args:
            timeout (float): The time in seconds to wait for termination for each unit.
        """
        self.join_normal(timeout)
        # Join all units
        for name in self.execution_order:
            self.units[name].obj.join_normal(timeout)

    async def join_units_async(self, timeout=None, interval=0.0):
        """Asynchronously wait until all the units terminate.

        Args:
            timeout (float): The time in seconds to wait for termination for each unit.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        async_tasks = []
        for name in self.execution_order:
            async_tasks.append(self.units[name].obj.join_async_task(timeout, interval))

        for async_task in async_tasks:
            await async_task

    async def join_all_async(self, timeout=None, interval=0.0):
        """Asynchronously wait until this object and all the units terminate.

        Args:
            timeout (float): The time in seconds to wait for termination for each unit.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        join_task = self.join_async_task()

        async_tasks = []
        for name in self.execution_order:
            async_tasks.append(self.units[name].obj.join_async_task(timeout, interval))

        for async_task in async_tasks:
            await async_task

        await join_task

    # Full Execution
    def join_units(self, timeout=None, interval=0.0):
        """Wait until all units terminate and determines if the async version should be run.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        # Use Correct Context
        if self.is_async():
            asyncio.run(self.join_units_async(timeout, interval))
        else:
            self.join_units_normal(timeout)

    def join_units_async_task(self, timeout=None, interval=0.0):
        """Creates waiting for all units to terminate as an asyncio task.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        return asyncio.create_task(self.join_units_async(timeout, interval))

    def join_all(self, timeout=None, interval=0.0):
        """Wait until this object and all units terminate and determines if the async version should be run.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        # Use Correct Context
        if self.is_async():
            asyncio.run(self.join_all_async(timeout, interval))
        else:
            self.join_all_normal(timeout)

    def join_all_async_task(self, timeout=None, interval=0.0):
        """Creates waiting for this object and all units to terminate as an asyncio task.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        return asyncio.create_task(self.join_all_async(timeout, interval))

    def terminate(self, join=False, timeout=None, interval=0.0):
        super().terminate(join, timeout, interval)

        # Terminate all units
        for name in self.execution_order:
            self.units[name].obj.terminate(join=False)

        # Join all units
        if join:
            for name in self.execution_order:
                self.units[name].join(timeout, interval)

    def reset(self):
        super().reset()
        # Reset all units
        for name in self.execution_order:
            self.units[name].reset()


# Processing #
class SeparateProcess(DynamicWrapper, ObjectWithLogging):
    _attributes_as_parents = ["_process"]
    CPU_COUNT = multiprocessing.cpu_count()
    class_loggers = {"separate_process": AdvancedLogger("separate_process")}

    # Construction/Destruction
    def __init__(self, target=None, name=None, daemon=False, init=False, kwargs={}):
        super().__init__()
        self._name = name
        self._daemon = daemon
        self._target = target
        self._target_kwargs = kwargs
        self.method_wrapper = run_method

        self._process = None

        if init:
            self.construct()

    @property
    def name(self):
        if self.process is not None:
            return self.process.name
        else:
            return self._name

    @name.setter
    def name(self, value):
        self._name = value
        if self.process is not None:
            self.process.name = value

    @property
    def daemon(self):
        if self.process is not None:
            return self.process.daemon
        else:
            return self._daemon

    @daemon.setter
    def daemon(self, value):
        self._daemon = value
        if self.process is not None:
            self.process.daemon = value

    @property
    def target(self):
        if self.process is not None:
            return self.process._target
        else:
            return self._target

    @target.setter
    def target(self, value):
        self._target = value
        if self.process is not None:
            self.process = Process(target=value, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    @property
    def target_kwargs(self):
        if self.process is not None:
            return self.process._kwargs
        else:
            return self._target_kwargs

    @target_kwargs.setter
    def target_kwargs(self, value):
        self._target_kwargs = value
        if self.process is not None:
            self.process = Process(target=self._target, name=self.name, daemon=self.daemon, kwargs=value)

    @property
    def process(self):
        return self._process

    @process.setter
    def process(self, value):
        self._process = value
        self._name = value.name
        self._daemon = value.daemon
        self._target = value._target
        self._target_kwargs = value._kwargs

    # Constructors
    def construct(self, target=None, daemon=False, **kwargs):
        self.create_process(target, daemon, **kwargs)

    # State
    def is_alive(self):
        if self.process is None:
            return False
        else:
            return self.process.is_alive()

    # Process
    def create_process(self, target=None, daemon=False, **kwargs):
        if target is not None:
            self.target = target
        if kwargs:
            self.target_kwargs = kwargs
        if daemon is not None:
            self.daemon = daemon
        self.process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    def set_process(self, process):
        self.process = process

    # Task
    def target_object_method(self, obj, method, kwargs={}):
        kwargs["obj"] = obj
        kwargs["method"] = method
        self.target_kwargs = kwargs
        self.process = Process(target=self.method_wrapper, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    # Execution
    def start(self):
        self.trace_log("separate_process", "start", "spawning new process...", name=self.name, level="DEBUG")
        self.process.start()

    async def join_async(self, timeout=None, interval=0.0):
        start_time = time.perf_counter()
        while self.process.join(0) is None:
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

    def restart(self):
        if isinstance(self.process, Process):
            if self.process.is_alive():
                self.terminate()
            self.process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)
        else:
            pass

    def close(self):
        if isinstance(self.process, Process):
            if self.process.is_alive():
                self.terminate()
            self.process.close()


class ProcessingUnit(DynamicWrapper, ObjectWithLogging):
    _attributes_as_parents = ["_task_object"]
    class_loggers = {"processor_root": AdvancedLogger("processor_root")}
    DEFAULT_TASK = Task

    # Construction/Destruction
    def __init__(self, name=None, task=None, to_kwargs={},
                 separate_process=False, daemon=False, p_kwargs={},
                 allow_setup=False, allow_closure=False, init=True):
        super().__init__()
        self.name = name
        self.unit_setup_kwargs = {}
        self.unit_closure_kwargs = {}
        self.allow_setup = allow_setup
        self.allow_closure = allow_closure
        self.await_closure = False

        self.separate_process = separate_process
        self._is_processing = False
        self.process = None
        self.processing_pool = None

        self._execute_setup = self.setup
        self._task_object = None
        self._execute_closure = self.closure
        self._joined = True

        if init:
            self.construct(name=name, task=task, to_kwargs=to_kwargs, daemon=daemon, p_kwargs=p_kwargs)

    @property
    def task_object(self):
        if self.is_processing():
            warnings.warn()
        return self._task_object

    @task_object.setter
    def task_object(self, value):
        if self.is_processing():
            warnings.warn()
        self._task_object = value

    # Constructors
    def construct(self, name=None, task=None, to_kwargs={}, daemon=False, p_kwargs={}):
        if self.separate_process:
            self.new_process(name=name, daemon=daemon, kwargs=p_kwargs)
        if task is None:
            if self.task_object is None:
                self.default_task_object(**to_kwargs)
        else:
            self.task_object = task

    # State
    def is_async(self):
        if asyncio.iscoroutinefunction(self._execute_setup) or asyncio.iscoroutinefunction(self._execute_closure):
            return True
        elif not self.separate_process and self.task_object.is_async():
            return True
        else:
            return False

    def is_processing(self):
        if self.process is not None and self.separate_process:
            self._is_processing = self.process.is_alive()
        return self._is_processing

    # Process
    def new_process(self, name=None, daemon=False, kwargs={}):
        if name is None:
            name = self.name
        self.process = SeparateProcess(name=name, daemon=daemon, kwargs=kwargs)

    def set_process(self, process):
        self.process = process

    # Set Task Object
    def default_task_object(self, **kwargs):
        self.task_object = self.DEFAULT_TASK(name=self.name, **kwargs)

    # Setup
    def setup(self):
        self.trace_log("processor_root", "setup", "setup method not overridden", name=self.name, level="DEBUG")

    # Closure
    def closure(self):
        self.trace_log("processor_root", "closure", "closure method not overridden", name=self.name, level="DEBUG")

    # Normal Execution Methods
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            self.trace_log("processor_root", "run_normal", "running setup", name=self.name, level="DEBUG")
            self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.trace_log("processor_root", "run_normal", "running task in separate process",
                           name=self.name, level="DEBUG")
            self.process.target_object_method(self.task_object, "run", kwargs=kwargs)
            self.process.start()
        else:
            self.trace_log("processor_root", "run_normal", "running task", name=self.name, level="DEBUG")
            self.task_object.run(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            self.trace_log("processor_root", "run_normal", "waiting for process to join (blocking)",
                           name=self.name, level="DEBUG")
            if self.separate_process:
                if self.await_closure:
                    self.process.join()
                else:
                    warnings.warn("Run Though! Process could still be running", self.name)
            self.trace_log("processor_root", "run_normal", "running closure", name=self.name, level="DEBUG")
            self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    def start_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            self.trace_log("processor_root", "start_normal", "running setup", name=self.name, level="DEBUG")
            self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.trace_log("processor_root", "start_normal", "starting task in separate process",
                           name=self.name, level="DEBUG")
            self.process.target_object_method(self.task_object, "start", kwargs=kwargs)
            self.process.start()
        else:
            self.trace_log("processor_root", "start_normal", "starting task", name=self.name, level="DEBUG")
            self.task_object.start(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            self.trace_log("processor_root", "start_normal", "waiting for process to join (blocking)",
                           name=self.name, level="DEBUG")
            if self.separate_process:
                if self.await_closure:
                    self.process.join()
                else:
                    warnings.warn("Run Though! Process could still be running")
            self.trace_log("processor_root", "start_normal", "running closure", name=self.name, level="DEBUG")
            self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    # Async Execute Methods
    async def run_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            if asyncio.iscoroutinefunction(self._execute_setup):
                self.trace_log("processor_root", "run_coro", "running async setup", name=self.name, level="DEBUG")
                await self._execute_setup(**self.unit_setup_kwargs)
            else:
                self.trace_log("processor_root", "run_coro", "running setup", name=self.name, level="DEBUG")
                self._execute_setup(**self.unit_setup_kwargs)

        # Todo: replace logs from here
        # Run Task
        if self.separate_process:
            self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running task in separate process", self.name))
            self.process.target_object_method(self.task_object, "run", kwargs=kwargs)
            self.process.start()
        else:
            if self.task_object.is_async():
                self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running async task", self.name))
                await self.task_object.run_coro(**kwargs)
            else:
                self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running task", self.name))
                self.task_object.run(**kwargs)

            # Optionally run Closure
            if self.allow_closure:
                if self.separate_process:
                    if self.await_closure:
                        self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Awaiting process to join", self.name))
                        await self.process.join_async()
                    else:
                        warnings.warn("Run Though! Process could still be running")
                if asyncio.iscoroutinefunction(self._execute_closure):
                    self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running async closure", self.name))
                    await self._execute_closure(**self.unit_closure_kwargs)
                else:
                    self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running closure", self.name))
                    self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    async def start_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            if asyncio.iscoroutinefunction(self._execute_setup):
                self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Running async setup", self.name))
                await self._execute_setup(**self.unit_setup_kwargs)
            else:
                self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Running setup", self.name))
                self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Starting task in separate process", self.name))
            self.process.target_object_method(self.task_object, "start", kwargs=kwargs)
            self.process.start()
        else:
            if self.task_object.is_async():
                self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Starting async task", self.name))
                await self.task_object.start_coro(**kwargs)
            else:
                self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Starting task", self.name))
                self.task_object.start(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if self.separate_process:
                if self.await_closure:
                    self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Awaiting process to join", self.name))
                    await self.process.join_async()
                else:
                    warnings.warn("Run though! Process could still be running")
            if asyncio.iscoroutinefunction(self._execute_closure):
                self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Running async closure", self.name))
                await self._execute_closure(**self.unit_closure_kwargs)
            else:
                self.loggers["processor_root"].debug(self.traceback_formatting("start_coro", "Running closure", self.name))
                self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    # Set Execution Methods
    def set_setup(self, func, kwargs={}):
        if kwargs:
            self.unit_setup_kwargs = kwargs
        self._execute_setup = func

    def use_task_setup(self):
        self.task_object.allow_setup = False
        self._execute_setup = self.task_object.setup

    def set_closure(self, func, kwargs={}):
        if kwargs:
            self.unit_closure_kwargs = kwargs
        self._execute_closure = func

    def use_task_closure(self):
        self.task_object.allow_closure = False
        self._execute_closure = self.task_object.closure

    # Execution
    def run(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        if self.is_async():
            asyncio.run(self.run_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.run_normal(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        return asyncio.create_task(self.run_coro(s_kwargs, t_kwargs, c_kwargs))

    def start(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        if self.is_async():
            asyncio.run(self.start_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.start_normal(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        return asyncio.create_task(self.start_coro(s_kwargs, t_kwargs, c_kwargs))

    def join(self, timeout=None):
        start_time = time.perf_counter()
        while not self._joined:
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

        if timeout is not None:
            timeout = timeout - (time.perf_counter() - start_time)

        if self.separate_process:
            self.process.join(timeout=timeout)
        return None

    async def join_async(self, timeout=None, interval=0.0):
        start_time = time.perf_counter()
        while not self._joined:
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

        if timeout is not None:
            timeout = timeout - (time.perf_counter() - start_time)

        if self.separate_process:
            await self.process.join_async(timeout=timeout, interval=interval)
        return None

    def stop(self, join=True, timeout=None):
        self.loggers["processor_root"].debug(self.traceback_formatting("stop", "Stopping process", self.name))
        self._task_object.stop()
        if join:
            self.join(timeout=timeout)

    async def stop_async(self, join=True, timeout=None, interval=0.0):
        self.loggers["processor_root"].debug(self.traceback_formatting("stop", "Stopping process asynchronously", self.name))
        self._task_object.stop()
        if join:
            await self.join_async(timeout=timeout, interval=interval)

    def reset(self):
        self.task_object.reset()

    def terminate(self):
        if self.separate_process:
            self.process.terminate()


class ProcessingCluster(ProcessingUnit):
    DEFAULT_TASK = MultiUnitTask

    # Construction/Destruction
    def __init__(self, name=None, task=None, to_kwargs={},
                 separate_process=False, daemon=False, p_kwargs={},
                 allow_setup=False, allow_closure=False, init=True):
        # Run Parent __init__ but only construct in child
        super().__init__(name=name, task=task, to_kwargs=to_kwargs,
                         separate_process=separate_process, daemon=daemon, p_kwargs=p_kwargs,
                         allow_setup=allow_setup, allow_closure=allow_closure, init=False)

        if init:
            self.construct(name)

    @property
    def execution_order(self):
        return self.task_object.execution_order

    @execution_order.setter
    def execution_order(self, value):
        self.task_object.execution_order = value

    # Container Magic Methods
    def __len__(self):
        return len(self.task_object)

    def __getitem__(self, item):
        return self.task_object[item]

    def __delitem__(self, key):
        del self.task_object[key]

    # Execution
    def stop(self, join=True, timeout=None):
        self.loggers["processor_root"].debug(self.traceback_formatting("stop", "Stopping process", self.name))
        self._task_object.stop(join=join, timeout=timeout)
        if join:
            self.join(timeout=timeout)

    async def stop_async(self, join=True, timeout=None, interval=0.0):
        self.loggers["processor_root"].debug(self.traceback_formatting("stop", "Stopping process asynchronously", self.name))
        self._task_object.stop(join=join, timeout=timeout)
        if join:
            await self.join_async(timeout=timeout, interval=interval)


# Functions #
def run_method(obj, method, **kwargs):
    return getattr(obj, method)(**kwargs)


# Main #
if __name__ == "__main__":
    pass
