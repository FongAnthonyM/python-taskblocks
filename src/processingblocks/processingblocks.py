#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processingblocks.py
Description:
"""
__author__ = "Anthony Fong"
__copyright__ = "Copyright 2021, Anthony Fong"
__credits__ = ["Anthony Fong"]
__license__ = ""
__version__ = "1.0.0"
__maintainer__ = "Anthony Fong"
__email__ = ""
__status__ = "Prototype"

# Default Libraries #
import asyncio
import multiprocessing
import multiprocessing.connection
from multiprocessing import Process, Pool, Lock, Event, Queue, Pipe
import warnings
import time

# Downloaded Libraries #
import advancedlogging
import dynamicwrapper

# Local Libraries #
from . import processingio


# Definitions #
# Classes #
class Task(advancedlogging.ObjectWithLogging):
    class_loggers = {"task_root": advancedlogging.AdvancedLogger("task_root")}

    # Construction/Destruction
    def __init__(self, name=None, allow_setup=True, allow_closure=True,
                 s_kwargs={}, t_kwargs={}, c_kwargs={}, init=True):
        super().__init__()
        self.name = name
        self.async_loop = asyncio.get_event_loop()
        self.setup_kwargs = s_kwargs
        self.task_kwargs = t_kwargs
        self.closure_kwargs = c_kwargs
        self.allow_setup = allow_setup
        self.allow_closure = allow_closure
        self.stop_event = Event()
        self.events = {}
        self.locks = {}

        self.inputs = None
        self.outputs = None

        self._execute_setup = self.setup
        self._execute_task = self.task
        self._execute_task_loop = self.task_loop
        self._execute_closure = self.closure

        if init:
            self.construct()

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
    def construct(self):
        self.construct_io()

    # State Methods
    def is_async(self):
        """ Check if this Task is using async.

        Returns:
            bool:
        """
        if asyncio.iscoroutinefunction(self._execute_setup) or asyncio.iscoroutinefunction(self._execute_closure) or \
           asyncio.iscoroutinefunction(self._execute_task):
            return True
        else:
            return False

    # IO
    def construct_io(self):
        """Constructs the io for this Task."""
        self.inputs = processingio.InputsHandler(name=self.name)
        self.outputs = processingio.OutputsHandler(name=self.name)

        self.inputs.create_event("SelfStop")

        self.create_io()

    def create_io(self):
        """Creates the individual io elements of this Task."""
        pass

    # Multiprocess Event
    def create_event(self, name):
        """ Creates a new event object and stores.

        Args:
            name (str): The name of the event.

        Returns:
            (obj:`Event`): The event
        """
        self.events[name] = Event()
        return self.events[name]

    def add_event(self, name, event):
        """ Adds and event to this object.

        Args:
            name:
            event:
        """
        self.events[name] = event

    # Multiprocess Lock
    def create_lock(self, name):
        self.locks[name] = Lock()
        return self.locks[name]

    def add_lock(self, name, lock):
        self.locks[name] = lock

    # Setup
    def setup(self, **kwargs):
        self.trace_log("task_root", "setup", "setup method not overridden", name=self.name, level="DEBUG")

    # Task
    def task(self, **kwargs):
        self.trace_log("task_root", "task", "task method not overridden", name=self.name, level="DEBUG")

    def task_loop(self, **kwargs):
        while not self.stop_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                self._execute_task(**kwargs)
            else:
                self.stop_event.set()

    async def task_loop_async(self, **kwargs):
        while not self.stop_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                await asyncio.create_task(self._execute_task(**kwargs))
            else:
                self.stop_event.set()

    # Closure
    def closure(self, **kwargs):
        self.trace_log("task_root", "closure", "closure method not overridden", name=self.name, level="DEBUG")

    # Normal Execute Methods
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            self.trace_log("task_root", "run_normal", "running setup", name=self.name, level="DEBUG")
            self._execute_setup(**self.setup_kwargs)

        # Run Task
        if t_kwargs:
            self.task_kwargs = t_kwargs
        self.trace_log("task_root", "run_normal", "running task", name=self.name, level="DEBUG")
        self._execute_task(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            self.trace_log("task_root", "run_normal", "running closure", name=self.name, level="DEBUG")
            self._execute_closure(**self.closure_kwargs)

    def start_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        # Prep
        self.prepare_task_loop()

        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            self.loggers["task_root"].debug(self.traceback_formatting("start_normal", "Running setup", self.name))
            self._execute_setup(**self.setup_kwargs)

        # Run Task Loop
        self.loggers["task_root"].debug(self.traceback_formatting("start_normal", "Running task", self.name))
        if t_kwargs:
            self.task_kwargs = t_kwargs
        self._execute_task_loop(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            self.loggers["task_root"].debug(self.traceback_formatting("start_normal", "Running closure", self.name))
            self._execute_closure(**self.closure_kwargs)

    # Async Execute Methods
    async def run_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            if asyncio.iscoroutinefunction(self._execute_setup):
                self.loggers["task_root"].debug(self.traceback_formatting("run_coro", "Running async setup", self.name))
                await self._execute_setup(**self.setup_kwargs)
            else:
                self.loggers["task_root"].debug(self.traceback_formatting("run_coro", "Running setup", self.name))
                self._execute_setup(**self.setup_kwargs)

        # Run Task
        if t_kwargs:
            self.task_kwargs = t_kwargs
        if asyncio.iscoroutinefunction(self._execute_task):
            self.loggers["task_root"].debug(self.traceback_formatting("run_coro", "Running async task", self.name))
            await self._execute_task(**self.task_kwargs)
        else:
            self.loggers["task_root"].debug(self.traceback_formatting("run_coro", "Running task", self.name))
            self._execute_task(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            self.loggers["task_root"].debug(self.traceback_formatting("run_coro", "Running closure", self.name))
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            if asyncio.iscoroutinefunction(self._execute_closure):
                self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Running async closure", self.name))
                await self._execute_closure(**self.closure_kwargs)
            else:
                self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Running closure", self.name))
                self._execute_closure(**self.closure_kwargs)

    async def start_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task_loop()

        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            if asyncio.iscoroutinefunction(self._execute_setup):
                self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Running async setup", self.name))
                await self._execute_setup(**self.setup_kwargs)
            else:
                self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Running setup", self.name))
                self._execute_setup(**self.setup_kwargs)

        # Run Task Loop
        if t_kwargs:
            self.task_kwargs = t_kwargs
        if asyncio.iscoroutinefunction(self._execute_task):
            self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Starting async task", self.name))
            await self._execute_task_loop(**self.task_kwargs)
        else:
            self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Starting task", self.name))
            self._execute_task_loop(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            if asyncio.iscoroutinefunction(self._execute_closure):
                self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Running async closure", self.name))
                await self._execute_closure(**self.closure_kwargs)
            else:
                self.loggers["task_root"].debug(self.traceback_formatting("start_coro", "Running closure", self.name))
                self._execute_closure(**self.closure_kwargs)

    # Set Execution Methods
    def set_setup(self, func, kwargs={}):
        if kwargs:
            self.setup_kwargs = kwargs
        self._execute_setup = func

    def set_task(self, func, kwargs={}):
        if kwargs:
            self.task_kwargs = kwargs
        self._execute_task = func

    def set_task_loop(self, func):
        self._execute_task_loop = func

    def prepare_task_loop(self):
        if self._execute_task_loop == self.task_loop:
            if asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop_async
        elif self._execute_task_loop == self.task_loop_async:
            if not asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop

    def set_closure(self, func, kwargs={}):
        if kwargs:
            self.closure_kwargs = kwargs
        self._execute_closure = func

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

    def stop(self):
        self.loggers["task_root"].debug(self.traceback_formatting("stop", "Stop was called", self.name))
        self.stop_event.set()
        self.inputs.stop_all()
        self.outputs.stop_all()

    def reset(self):
        self.stop_event.clear()


class MultiUnitTask(Task):
    # class_loggers = Task.class_loggers.copy().update({"test": AdvanceLogger("test")})
    SETTING_NAMES = {"unit", "start", "setup", "closure", "s_kwargs", "t_kwargs", "c_kwargs"}

    # Construction/Destruction
    def __init__(self, name=None, units={}, execution_kwargs={}, order=(),
                 allow_setup=True, allow_closure=True, s_kwargs={}, t_kwargs={}, c_kwargs={}, init=True):
        # Run Parent __init__ but only construct in child
        super().__init__(name=name, allow_setup=allow_setup, allow_closure=allow_closure,
                         s_kwargs=s_kwargs, t_kwargs=t_kwargs, c_kwargs=c_kwargs, init=False)

        # Attributes
        self._execution_order = ()

        self.units = {}
        self.execution_kwargs = {}

        # Optionally Construct this object
        if init:
            self.construct(units=units, execution_kwargs=execution_kwargs, order=order)

    @property
    def execution_order(self):
        return self._execution_order

    @execution_order.setter
    def execution_order(self, value):
        if len(value) == len(self.units):
            self._execution_order = value
        else:
            warnings.warn()

    # Container Methods
    def __len__(self):
        return len(self.units)

    def __getitem__(self, item):
        return self.units[item]

    def __delitem__(self, key):
        del self.execution_kwargs[key]
        del self.units[key]

    # Constructors/Destructors
    def construct(self, units={}, execution_kwargs={}, order=()):
        super().construct()
        if units:
            self.extend(units=units, execution_kwargs=execution_kwargs)
        if order:
            self.execution_order = order

    # Container Methods
    def keys(self):
        return self.units.keys()

    def values(self):
        return self.units.values()

    def items(self):
        return self.units.items()

    def set_unit(self, name, unit, start=True, setup=False, closure=False, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.units[name] = unit
        self.execution_kwargs[name] = {"start": start, "setup": setup, "closure": closure,
                                       "s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}

    def extend(self, units, execution_kwargs={}):
        if execution_kwargs:
            if set(execution_kwargs).issubset(units):
                for name, unit in units.items():
                    if set(execution_kwargs[name]).issubset(self.SETTING_NAMES):
                        self.set_unit(name, unit, **execution_kwargs[name])
                    else:
                        warnings.warn()
                else:
                    warnings.warn()
        else:
            for name, unit in units.items():
                self.set_unit(name, unit)

    def pop(self, name):
        del self.execution_kwargs[name]
        return self.units.pop(name)

    def clear(self):
        self.execution_kwargs.clear()
        self.units.clear()

    def all_async(self):
        for unit in self.units.values():
            if not unit.is_async():
                return False
        return True

    def any_async(self):
        for unit in self.units.values():
            if unit.is_async():
                return True
        return False

    # Setup
    def setup(self):
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            execution_kwargs = self.execution_kwargs[name]
            if execution_kwargs["setup"]:
                self.loggers["task_root"].debug(self.traceback_formatting("setup", "Running %s setup" % unit.name, self.name))
                unit.allow_setup = False
                unit.setup(**execution_kwargs["c_kwargs"])
            if execution_kwargs["closure"]:
                unit.allow_closure = False

    # Task
    def task(self):
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            start = self.execution_kwargs[name]["start"]
            s_kwargs = self.execution_kwargs[name]["s_kwargs"]
            t_kwargs = self.execution_kwargs[name]["t_kwargs"]
            c_kwargs = self.execution_kwargs[name]["c_kwargs"]
            if start:
                unit.start(s_kwargs, t_kwargs, c_kwargs)
            else:
                unit.run(s_kwargs, t_kwargs, c_kwargs)

    async def task_async(self):
        tasks = []
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            start = self.execution_kwargs[name]["start"]
            s_kwargs = self.execution_kwargs[name]["s_kwargs"]
            t_kwargs = self.execution_kwargs[name]["t_kwargs"]
            c_kwargs = self.execution_kwargs[name]["c_kwargs"]
            if start:
                if unit.is_async():
                    tasks.append(unit.start_async_task(s_kwargs, t_kwargs, c_kwargs))
                else:
                    unit.start(s_kwargs, t_kwargs, c_kwargs)
            else:
                if unit.is_async():
                    tasks.append(unit.run_async_task(s_kwargs, t_kwargs, c_kwargs))
                else:
                    unit.run(s_kwargs, t_kwargs, c_kwargs)

        for task in tasks:
            await task

    # Closure
    def closure(self):
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            execution_kwargs = self.execution_kwargs[name]
            if execution_kwargs["closure"]:
                unit.closure(**execution_kwargs["c_kwargs"])

    # Set Execution Methods
    def prepare_task(self):
        if self._execute_task == self.task:
            if self.any_async():
                self._execute_task = self.task_async
        elif self._execute_task == self.task_async:
            if not self.any_async():
                self._execute_task = self.task

    # Execution
    def run(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        super().run(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        return super().run_async_task(s_kwargs, t_kwargs, c_kwargs)

    def start(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        super().start(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        return super().start_async_task(s_kwargs, t_kwargs, c_kwargs)

    def stop(self, join=True, timeout=None):
        super().stop()
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            self.units[name].stop(join=False)

        for name in names:
            self.units[name].join(timeout=timeout)

    def reset(self):
        super().reset()
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            self.units[name].reset()


class SeparateProcess(dynamicwrapper.DynamicWrapper, advancedlogging.ObjectWithLogging):
    _attributes_as_parents = ["_process"]
    CPU_COUNT = multiprocessing.cpu_count()
    class_loggers = {"separate_process": advancedlogging.AdvancedLogger("separate_process")}

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
        self.loggers["separate_process"].debug(self.traceback_formatting("start", "Spawning new process...", self.name))
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


class ProcessingUnit(dynamicwrapper.DynamicWrapper, advancedlogging.ObjectWithLogging):
    _attributes_as_parents = ["_task_object"]
    class_loggers = {"processor_root": advancedlogging.AdvancedLogger("processor_root")}
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
        self.loggers["processor_root"].debug(self.traceback_formatting("setup", "setup method not overridden", self.name))

    # Closure
    def closure(self):
        self.loggers["processor_root"].debug(self.traceback_formatting("closure", "closure method not overridden", self.name))

    # Normal Execution Methods
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            self.loggers["processor_root"].debug(self.traceback_formatting("run_normal", "Running setup", self.name))
            self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.loggers["processor_root"].debug(self.traceback_formatting("run_normal", "Running task in separate process", self.name))
            self.process.target_object_method(self.task_object, "run", kwargs=kwargs)
            self.process.start()
        else:
            self.loggers["processor_root"].debug(self.traceback_formatting("run_normal", "Running task", self.name))
            self.task_object.run(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            self.loggers["processor_root"].debug(self.traceback_formatting("start_normal", "Waiting for process to join (Blocking)", self.name))
            if self.separate_process:
                if self.await_closure:
                    self.process.join()
                else:
                    warnings.warn("Run Though! Process could still be running", self.name)
            self.loggers["processor_root"].debug(self.traceback_formatting("run_normal", "Running closure", self.name))
            self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    def start_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            self.loggers["processor_root"].debug(self.traceback_formatting("start_normal", "Running setup", self.name))
            self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.loggers["processor_root"].debug(self.traceback_formatting("start_normal", "Starting task in separate process", self.name))
            self.process.target_object_method(self.task_object, "start", kwargs=kwargs)
            self.process.start()
        else:
            self.loggers["processor_root"].debug(self.traceback_formatting("run_normal", "Starting task"))
            self.task_object.start(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            self.loggers["processor_root"].debug(self.traceback_formatting("start_normal", "Waiting for process to join (Blocking)", self.name))
            if self.separate_process:
                if self.await_closure:
                    self.process.join()
                else:
                    warnings.warn("Run Though! Process could still be running")
            self.loggers["processor_root"].debug(self.traceback_formatting("start_normal", "Running closure", self.name))
            self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    # Async Execute Methods
    async def run_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            if asyncio.iscoroutinefunction(self._execute_setup):
                self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running async setup", self.name))
                await self._execute_setup(**self.unit_setup_kwargs)
            else:
                self.loggers["processor_root"].debug(self.traceback_formatting("run_coro", "Running setup", self.name))
                self._execute_setup(**self.unit_setup_kwargs)

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
