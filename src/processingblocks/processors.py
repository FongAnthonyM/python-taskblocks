#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processors.py
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
import typing

# Downloaded Libraries #
import advancedlogging
from advancedlogging import AdvancedLogger, ObjectWithLogging
from baseobjects import StaticWrapper

# Local Libraries #
from .tasks import Task, MultiUnitTask


# Definitions #
# Classes #
# Processing #
class SeparateProcess(ObjectWithLogging, StaticWrapper):
    _wrapped_types = [Process()]
    _wrap_attributes = ["_process"]
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
        if self._process is not None:
            return self._process.name
        else:
            return self._name

    @name.setter
    def name(self, value):
        self._name = value
        if self._process is not None:
            self._process.name = value

    @property
    def daemon(self):
        if self._process is not None:
            return self._process.daemon
        else:
            return self._daemon

    @daemon.setter
    def daemon(self, value):
        self._daemon = value
        if self._process is not None:
            self._process.daemon = value

    @property
    def target(self):
        if self.process is not None:
            return self._process._target
        else:
            return self._target

    @target.setter
    def target(self, value):
        self._target = value
        if self._process is not None:
            self._process = Process(target=value, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    @property
    def target_kwargs(self):
        if self._process is not None:
            return self._process._kwargs
        else:
            return self._target_kwargs

    @target_kwargs.setter
    def target_kwargs(self, value):
        self._target_kwargs = value
        if self._process is not None:
            self._process = Process(target=self._target, name=self.name, daemon=self.daemon, kwargs=value)

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
        if self._process is None:
            return False
        else:
            return self._process.is_alive()

    # Process
    def create_process(self, target=None, daemon=False, **kwargs):
        if target is not None:
            self.target = target
        if kwargs:
            self.target_kwargs = kwargs
        if daemon is not None:
            self.daemon = daemon
        self._process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

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
        self._process.start()

    async def join_async(self, timeout=None, interval=0.0):
        start_time = time.perf_counter()
        while self._process.join(0) is None:
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

    def join_async_task(self, timeout=None, interval=0.0):
        """Creates waiting for this object to terminate as an asyncio task.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        return asyncio.create_task(self.join_async(timeout, interval))

    def restart(self):
        if isinstance(self._process, Process):
            if self.process.is_alive():
                self._process.terminate()
        self._process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)
        self._process.start()

    def close(self):
        if isinstance(self.process, Process):
            if self._process.is_alive():
                self._process.terminate()
            self._process.close()


class ProcessingUnit(ObjectWithLogging, StaticWrapper):
    _wrapped_types = [Task()]
    _wrap_attributes = ["_task_object"]
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

