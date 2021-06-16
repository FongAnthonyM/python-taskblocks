#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" tasks.py
Description:
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
import dataclasses
from multiprocessing import Process, Pool, Lock, Event, Queue, Pipe
import warnings
import time
import typing

# Downloaded Libraries #
from advancedlogging import AdvancedLogger, ObjectWithLogging
from baseobjects import TimeoutWarning

# Local Libraries #
from .io import InputsHandler, OutputsHandler
from .processors import SeparateProcess


# Definitions #
# Classes #
# Tasks #
# Todo: Maybe add some warning when editing a Task while it is running.
class Task(ObjectWithLogging):
    """The most basic block unit.

    A Task is the basic block unit and is a self contained

    Class Attributes:
        class_loggers (dict): The default loggers to include in every object of this class.

    Attributes:
        loggers (dict): A collection of loggers used by this object. The keys are the names of the different loggers.
        name (str): The name of this object.
        async_loop: The event loop to assign the async methods to.
        _is_async (bool): Determines if this object will be asynchronous.
        _is_process (bool): Determines if this object will run in a separate process.
        allow_setup (bool): Determines if setup will be run.
        allow_closure (bool): Determines if closure will be run.
        use_async (bool): Determines if async methods will be used by default.
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
        self._is_async = True
        self._is_process = False
        self.allow_setup = True
        self.allow_closure = True
        self.use_async = True
        self.alive_event = Event()
        self.terminate_event = Event()
        self.events = {}
        self.locks = {}

        self.setup_kwargs = {}
        self.task_kwargs = {}
        self.closure_kwargs = {}

        self.inputs = None
        self.outputs = None

        self.process = None

        self._execute_setup = None
        self._execute_task = None
        self._execute_task_loop = None
        self._execute_closure = None

        if init:
            self.construct(name, allow_setup, allow_closure, s_kwargs, t_kwargs, c_kwargs)

    @property
    def is_async(self):
        """bool: If this object is asynchronous. It will detect if it asynchronous while running.

        When set it will raise an error if the Task is running.
        """
        if self.is_alive():
            if asyncio.iscoroutinefunction(self._execute_task) or asyncio.iscoroutinefunction(self._execute_setup) or \
               asyncio.iscoroutinefunction(self._execute_closure):
                return True
            else:
                return False
        else:
            return self._is_async

    @is_async.setter
    def is_async(self, value):
        self._is_async = value
        if self.is_alive():
            raise ValueError  # Todo: Handle this

    @property
    def is_process(self):
        """bool: If this object will run in a separate process. It will detect if it is in a process while running.

        When set it will raise an error if the Task is running.
        """
        if self.is_alive():
            if self.process is not None and self.process.is_alive():
                return True
            else:
                return False
        else:
            return self._is_process

    @is_process.setter
    def is_process(self, value):
        self._is_process = value
        if self.is_alive():
            raise ValueError  # Todo: Handle this

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

        self.build_loggers()
        self.construct_io()

    # State Methods
    def is_alive(self):
        """Checks if this object is currently running.

        Returns:
            bool: If this object is currently running.
        """
        return self.alive_event.is_set()

    def is_processing(self):
        """Checks if this object is currently running in a process.

        Returns:
            bool: If this object is currently running in a process.
        """
        try:
            return self.process.is_alive()
        except AttributeError:
            return False

    # IO
    def construct_io(self):
        """Constructs the io for this object."""
        self.inputs = InputsHandler(name=self.name)
        self.outputs = OutputsHandler(name=self.name)

        self.inputs.create_event("SelfStop")

        self.create_io()

    def create_io(self):
        """Abstract method that creates the individual io elements for this object."""
        pass

    def link_inputs(self, **kwargs):
        """Abstract method that gives a place to the inputs to other objects."""
        pass

    def link_outputs(self, **kwargs):
        """Abstract method that gives a place to the outputs to other objects."""
        pass

    def link_io(self, **kwargs):
        """Abstract method that gives a place to the io to other objects."""
        pass

    # Separate Process
    def create_process(self, target=None, args=(), kwargs={}, daemon=None, delay=True):
        """Creates a separate process for this task.

        Args:
            target: The function that will be executed by the separate process.
            args: The arguments for the function to be run in the separate process.
            kwargs: The keyword arguments for the function to be run the in the separate process.
            daemon (bool): Determines if the separate process will continue after the main process exits.
            delay (bool): Determines if the Process will be constructed now or later.
        """
        self.process = SeparateProcess(target, self.name, args, kwargs, daemon, delay)

    def set_process_method(self, method, args=(), kwargs={}):
        """Sets the separate process's target to a method from this object.

        Args:
            method (str): The name of the method to run from this object.
            args: The arguments for the function to be run in the separate process.
            kwargs: The keyword arguments for the function to be run the in the separate process.

        Returns:

        """
        self.process.target_object_method(self, method, args, kwargs)

    # Event
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

    # Lock
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
        while not self.terminate_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                self._execute_task(**kwargs)
            else:
                self.terminate_event.set()

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
        """Sets the task loop function to run.

        Args:
            func: The function which will be used to loop.
        """
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
    def prepare(self):
        """Sets the execute methods."""
        if self._execute_setup is None:
            self._execute_setup = self.setup

        if self._execute_task is None:
            if self.use_async:
                self._execute_task = self.task_async
            else:
                self._execute_task = self.task

        if self._execute_task_loop is None:
            # Change the task loop to async if the task is async
            if asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop_async
            # Change the task loop to a normal method if the task is a normal method
            else:
                self._execute_task_loop = self.task_loop

        if self._execute_closure is None:
            self._execute_closure = self.closure

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
        if allow_closure is None:
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
        if allow_setup is None:
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
        if allow_closure is None:
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

    # Separate Process Execution
    def _execute_process(self, method, asyn=None, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Starts running a method from this object in separate process.

        Args:
            method (str): The name of the method to run in a separate process.
            asyn (bool): Determines if this object should run asynchronously.
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        kwargs = {"process": False,
                  "asyn": asyn,
                  "s_kwargs": s_kwargs,
                  "t_kwargs": t_kwargs,
                  "c_kwargs": c_kwargs}

        if self.process is None:
            self.create_process()
        self.set_process_method(method, kwargs=kwargs)
        self.process.start()

    async def _execute_process_async(self, method, asyn=None, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Asynchronously starts running a method from this object in separate process.

        Args:
            method (str): The name of the method to run in a separate process.
            asyn (bool): Determines if this object should run asynchronously.
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        self._execute_process(method, asyn, s_kwargs, t_kwargs, c_kwargs)

    # Normal Execution
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Executes a single run of the task with setup and closure.

        Args:
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Set async
        self._is_async = False

        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Set execute methods
        self.prepare()

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
        # Set async
        self._is_async = False

        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Set execute methods
        self.prepare()

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
        # Set async
        self._is_async = True

        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Set execute methods
        self.prepare()

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
        # Set async
        self._is_async = True

        # Flag On
        if not self.alive_event.is_set():
            self.alive_event.set()

        # Set execute methods
        self.prepare()

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
        if self.is_process:
            self.process.join(timeout)
        else:
            start_time = time.perf_counter()
            while self.alive_event.is_set():
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn(TimeoutWarning("'join_normal'"), stacklevel=2)
                    return

    async def join_async(self, timeout=None, interval=0.0):
        """Asynchronously wait until this object terminates.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        if self.is_process:
            await self.process.join_async(timeout, interval)
        else:
            start_time = time.perf_counter()
            while self.alive_event.is_set():
                await asyncio.sleep(interval)
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn(TimeoutWarning("'join_async'"), stacklevel=2)
                    return

    # Full Execution
    def run(self, process=None, asyn=None, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Executes a single run of the task and determines if the async version should be run.

        Args:
            process (bool): Determines if this object should run in a separate process.
            asyn (bool): Determines if this object should run asynchronously.
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Set separate process
        if process is not None:
            self._is_process = process

        # Set async
        if asyn is not None:
            self._is_async = asyn

        # Use Correct Context
        if self._is_process:
            self._execute_process("run", asyn, s_kwargs, t_kwargs, c_kwargs)
        elif self._is_async:
            asyncio.run(self.run_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.run_normal(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, process=None, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Creates a single execution of this task as an asyncio task.

        Args:
            process (bool): Determines if this object should run in a separate process.
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Set separate process
        if process is not None:
            self.is_process = process

        # Flag On
        self.alive_event.set()

        # Create Async Event
        if self._is_process:
            return asyncio.create_task(self._execute_process_async("run", None, s_kwargs, t_kwargs, c_kwargs))
        else:
            # Set async
            self._is_async = True

            return asyncio.create_task(self.run_coro(s_kwargs, t_kwargs, c_kwargs))

    def start(self, process=None, asyn=None, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Starts the execution of multiple runs of the task and determines if the async version should be run.

        Args:
            process (bool): Determines if this object should run in a separate process.
            asyn (bool): Determines if this object should run asynchronously.
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Set separate process
        if process is not None:
            self._is_process = process

        # Set async
        if asyn is not None:
            self._is_async = asyn

        # Use Correct Context
        if self._is_process:
            self._execute_process("start", asyn, s_kwargs, t_kwargs, c_kwargs)
        elif self._is_async:
            asyncio.run(self.start_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.start_normal(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, process=None, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        """Creates the continuous execution of this task as an asyncio task.

        Args:
            process (bool): Determines if this object should run in a separate process.
            s_kwargs (dict): The keyword arguments for the setup.
            t_kwargs (dict): The keyword arguments for the task.
            c_kwargs (dict): The keyword arguments for the closure.
        """
        # Set separate process
        if process is not None:
            self.is_process = process

        # Flag On
        self.alive_event.set()

        # Create Async Event
        if self._is_process:
            return asyncio.create_task(self._execute_process_async("start", None, s_kwargs, t_kwargs, c_kwargs))
        else:
            # Set async
            self._is_async = True

            return asyncio.create_task(self.start_coro(s_kwargs, t_kwargs, c_kwargs))

    def join(self, asyn=False, timeout=None, interval=0.0):
        """Wait until this object terminates and determines if the async version should be run.

        Args:
            asyn (bool): Determines if the join will be asynchronous.
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.

        Returns:
            Can return None or an async_io task object if this function is called with async on.
        """
        # Use Correct Context
        if asyn:
            return self.join_async_task(timeout, interval)
        else:
            self.join_normal(timeout)
            return None

    def join_async_task(self, timeout=None, interval=0.0):
        """Creates waiting for this object to terminate as an asyncio task.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        return asyncio.create_task(self.join_async(timeout, interval))

    def stop(self, join=False, asyn=False, timeout=None, interval=0.0):
        """Abstract method that should stop this task.

        Args:
            join (bool): Determines if terminate will wait for the object to join.
            asyn (bool): Determines if the join will be asynchronous.
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.

        Returns:
            Can return None or an async_io task object if this function is called with async on.
        """
        if join:
            return self.join(asyn, timeout, interval)

    def terminate(self, join=False, asyn=False, timeout=None, interval=0.0):
        """Flags the task loop and task to stop running.

        Args:
            join (bool): Determines if terminate will wait for the object to join.
            asyn (bool): Determines if the join will be asynchronous.
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.

        Returns:
            Can return None or an async_io task object if this function is called with async on.
        """
        self.trace_log("task_root", "terminate", "terminate was called", name=self.name, level="DEBUG")
        self.terminate_event.set()
        self.inputs.stop_all()
        self.outputs.stop_all()
        if join:
            return self.join(asyn, timeout, interval)

    def reset(self):
        """Resets the stop flag to allow this task to run again."""
        self.terminate_event.clear()


@dataclasses.dataclass
class TaskUnit:
    """A Dataclass that hold information about task units for """
    name: str
    obj: typing.Any
    execute_type: str
    pre_setup: bool
    post_closure: bool


class MultiUnitTask(Task):
    """A Task that contains and executes tasks within it.

    Class Attributes:
        class_loggers (dict): The default loggers to include in every object of this class.

    Attributes:
        loggers (dict): A collection of loggers used by this object. The keys are the names of the different loggers.
        name (str): The name of this object.
        async_loop: The event loop to assign the async methods to.
        allow_setup (bool): Determines if setup will be run.
        allow_closure (bool): Determines if closure will be run.
        use_async (bool): Determines if async methods will be used by default.
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

        _execution_order (:obj:`tuple` of :obj:`str`): The order to execute the contained units.
        units (:obj:`dict` of :obj:`TaskUnit`): The contained Tasks to execute by name.

    Args:
        units :obj:`dict` of :obj:`TaskUnit`): The contained Tasks to execute by name.
        order (:obj:`tuple` of :obj:`str`): The order of the units to be used by name.
        name (str, optional): Name of this object.
        allow_setup (bool, optional): Determines if setup will be run.
        allow_closure (bool, optional): Determines if closure will be run.
        s_kwargs (dict, optional): Contains the keyword arguments to be used in the setup method.
        t_kwargs (dict, optional): Contains the keyword arguments to be used in the task method.
        c_kwargs (dict, optional): Contains the keyword arguments to be used in the closure method.
        init (bool, optional): Determines if this object should be initialized.
    """
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
        """Gets the order to execute the units. If not set, it will return the current order of units.

        The set raises an error if the order does not include all of the units within it.
        """
        if self._execution_order:
            return self._execution_order
        else:
            return self.units.keys()

    @execution_order.setter
    def execution_order(self, value):
        if len(value) == len(self.units):
            self._execution_order = value
        else:
            raise IndexError("the execution order must contain all the units")

    # Container Methods
    def __len__(self):
        """The number of units this object has.

        Returns:
            int: The number of units this object has.
        """
        return len(self.units)

    def __getitem__(self, key):
        """Gets a unit contained in this object.

        Args:
            key (str): The name of the unit to get

        Returns:
            :obj:`Task`: The task requested.
        """
        if key in self.units:
            return self.units[key]
        raise KeyError(key)

    def __delitem__(self, key):
        """Deletes the unit within this object.

        Args:
            key (str): The name of the unit to delete.
        """
        del self.units[key]

    # Constructors/Destructors
    def construct(self, units={}, order=(), **kwargs):
        """Constructs this object

        Args:
            units: The tasks to execute.
            order (:obj:`tuple` of :obj:`str`): The order of the units to be used by name.
            name (str, optional): Name of this object.
            allow_setup (bool, optional): Determines if setup will be run.
            allow_closure (bool, optional): Determines if closure will be run.
            s_kwargs (dict, optional): Contains the keyword arguments to be used in the setup method.
            t_kwargs (dict, optional): Contains the keyword arguments to be used in the task method.
            c_kwargs (dict, optional): Contains the keyword arguments to be used in the closure method.
        """
        super().construct(**kwargs)
        if units:
            self.update(units=units)
        if order:
            self.execution_order = order

    # State
    def all_async(self):
        """Notifies if all the contained units are asynchronous.

        Returns:
            bool: If all the contained units are asynchronous.
        """
        for unit in self.units.values():
            if not unit.obj.is_async:
                return False
        return True

    def any_async(self):
        """Notifies if all the contained units are asynchronous.

        Returns:
            bool: If all the contained units are asynchronous.
        """
        for unit in self.units.values():
            if unit.obj.is_async:
                return True
        return False

    # Container Methods
    def keys(self):
        """Gets the keys of the units.

        Returns:
            The keys of the units.
        """
        return self.units.keys()

    def values(self):
        """Get all the units within this object.

        Returns:
            All of the units within this object.
        """
        return self.units.values()

    def items(self):
        """Gets the items, name and unit, within this object.

        Returns:
            The units and their names within this object
        """
        return self.units.items()

    def set_unit(self, name, obj, execution_type="run", pre_setup=False, post_closure=False):
        """Sets a single unit within this object.

        Args:
            name: The name of the unit.
            obj: The unit to store.
            execution_type: The type of execution of this unit to run.
            pre_setup: Determines if the setup should be run during this object's setup or during its execution.
            post_closure: Determines if the closure should be run during this object's closure or during its execution.
        """
        self.units[name] = TaskUnit(name, obj, execution_type, pre_setup, post_closure)

    def update(self, units):
        """Updates the contained unit dictionary with the new dictionary.

        Args:
            units (:obj:`dict` of :obj:`Task`): The new dictionary to add to the units of this object.
        """
        for name, unit in units.items():
            if isinstance(unit, TaskUnit):
                self.units[name] = unit
            elif isinstance(unit, dict):
                if "name" not in unit:
                    unit["name"] = name
                self.units[name] = TaskUnit(**unit)

    def pop(self, name):
        """Removes the named unit from this object.

        Args:
            name: The name of the tasks unit to remove from this object.

        Returns:
            A
        """
        return self.units.pop(name)

    def clear(self):
        """Clears the task units from this object."""
        self.units.clear()

    # Setup
    def setup(self, **kwargs):
        """Setup this object and the contained units if they are setting up early."""
        # Execute the setup of the units if they have pre-setup
        for name in self.execution_order:
            unit = self.units[name]
            if unit.pre_setup:
                unit.obj.execute_setup(allow_setup=True)

    # Task
    def task(self, **kwargs):
        """The main method to execute the contained units."""
        # Execute all of the units
        for name in self.execution_order:
            unit = self.units[name]
            if unit.execute_type == "start":
                unit.obj.start()
            else:
                unit.obj.run()

    async def task_async(self):
        """The main async method to execute the contained units."""
        async_tasks = []
        sequential_executions = []

        # Create objects to execute
        for name in self.execution_order:
            unit = self.units[name]
            obj = unit.obj
            if unit.execute_type == "start":
                if obj.is_async:
                    async_tasks.append(obj.start_async_task())
                else:
                    sequential_executions.append(obj.start)
            else:
                if obj.is_async:
                    async_tasks.append(obj.run_async_task())
                else:
                    sequential_executions.append(obj.run)

        # Execute all of the units
        for execute in sequential_executions:
            execute()

        await asyncio.gather(*async_tasks)

    # Closure
    def closure(self, **kwargs):
        """Close this object and the contained units if they delayed their closure."""
        # Execute the setup of the units if they have post-closure
        for name in self.execution_order:
            unit = self.units[name]
            if unit.post_closure:
                unit.obj.execute_closure(allow_closure=True)

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
        if self.is_async:
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
        if self.is_async:
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

    def terminate(self, join=False, asyn=False, timeout=None, interval=0.0):
        """Flags the task loop and task to stop running.

        Args:
            join (bool): Determines if terminate will wait for the object to join.
            asyn (bool): Determines if the join will be asynchronous.
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        super().terminate(join, asyn, timeout, interval)

        # Terminate all units
        for name in self.execution_order:
            self.units[name].obj.terminate(join=False)

        # Join all units
        if join:
            for name in self.execution_order:
                self.units[name].join(timeout, interval)

    def reset(self):
        """Resets the stop flag to allow this task to run again."""
        super().reset()
        # Reset all units
        for name in self.execution_order:
            self.units[name].reset()

