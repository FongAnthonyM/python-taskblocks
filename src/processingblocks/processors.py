#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processors.py
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
import copy
import multiprocessing
import multiprocessing.connection
from multiprocessing import Process, Pool, Lock, Event, Queue, Pipe
import warnings
import time
import typing

# Downloaded Libraries #
import advancedlogging
from advancedlogging import AdvancedLogger, ObjectWithLogging
from baseobjects import StaticWrapper, TimeoutWarning

# Local Libraries #


# Definitions #
# Functions #
def run_method(obj, method, **kwargs):
    return getattr(obj, method)(**kwargs)


# Classes #
# Processing #
# Todo: Add automatic closing to release resources automatically
class SeparateProcess(ObjectWithLogging, StaticWrapper):
    """A wrapper for Process which adds restarting and passing a method to be run in a separate process.

    Class Attributes:
        CPU_COUNT (int): The number of CPUs this computer has.
        class_loggers (:obj:`dict` of :obj:`AdvancedLogger`): The loggers for this class.

    Attributes:
        name (str): The name of this object.
        daemon (bool): Determines if the separate process will continue after the main process exits.
        target: The function that will be executed by the separate process.
        args: The arguments for the function to be run in the separate process.
        kwargs: The keyword arguments for the function to be run the in the separate process.
        method_wrapper: A wrapper function which executes a method of an object.
        _process (:obj:`Process`): The Process object that this object is wrapping.

    Args:
        target: The function that will be executed by the separate process.
        name: The name of this object.
        args: The arguments for the function to be run in the separate process.
        kwargs: The keyword arguments for the function to be run the in the separate process.
        daemon (bool): Determines if the separate process will continue after the main process exits.
        delay (bool): Determines if the Process will be constructed now or later.
        init (bool): Determines if this object will construct.
    """
    _wrapped_types = [Process(daemon=True)]
    _wrap_attributes = ["_process"]
    CPU_COUNT = multiprocessing.cpu_count()
    class_loggers = {"separate_process": AdvancedLogger("separate_process")}

    # Construction/Destruction
    def __init__(self, target=None, name="", args=(), kwargs={}, daemon=None, delay=False, init=True):
        super().__init__()
        self.method_wrapper = run_method

        self._process = None

        if init:
            self.construct(target, name, args, kwargs, daemon, delay)

    @property
    def target(self):
        """The function that will be executed by the separate process, gets from _process."""
        return self._target

    @target.setter
    def target(self, value):
        self._target = value

    @property
    def args(self):
        """The arguments for the function to be run in the separate process, gets from _process."""
        return self._args

    @args.setter
    def args(self, value):
        self._args = tuple(value)

    @property
    def kwargs(self):
        """The keyword arguments for the function to be run the in the separate process, get from _process."""
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = dict(value)

    # Pickling
    def __getstate__(self):
        """Creates a dictionary of attributes which can be used to rebuild this object.

        Returns:
            dict: A dictionary of this object's attributes.
        """
        out_dict = self.__dict__.copy()
        process = out_dict["__process"]
        if process:
            kwargs = {"target": process._target,
                      "name": process._name,
                      "args": process._args,
                      "kwargs": process._kwargs}
            try:
                kwargs["daemon"] = process.daemon
            except AttributeError:
                pass
        else:
            kwargs = None

        out_dict["new_process_kwargs"] = kwargs
        out_dict["__process"] = None

        return out_dict

    def __setstate__(self, in_dict):
        """Builds this object based on a dictionary of corresponding attributes.

        Args:
            in_dict (dict): The attributes to build this object from.
        """
        process_kwargs = in_dict.pop("new_process_kwargs")
        self.__dict__ = in_dict
        if process_kwargs:
            self.create_process(**process_kwargs)

    # Constructors
    def construct(self, target=None, name=None, args=(), kwargs={}, daemon=None, delay=False):
        """Constructs this object.

        Args:
            target: The function that will be executed by the separate process.
            name: The name of this object.
            args: The arguments for the function to be run in the separate process.
            kwargs: The keyword arguments for the function to be run the in the separate process.
            daemon (bool): Determines if the separate process will continue after the main process exits.
            delay (bool): Determines if the Process will be constructed now or later.
        """
        if name is not None:
            self.name = name

        # Create process if not delayed
        if not delay:
            self.create_process(target, name, args, kwargs, daemon)
        # Stash the attributes until a new process is made
        else:
            if target is not None:
                self._target = target

            if name is not None:
                self._name = name

            if args:
                self._args = tuple(args)

            if kwargs:
                self._kwargs = dict(kwargs)

            if daemon:
                self.daemon = daemon

    # State
    def is_alive(self):
        """Checks if the process is running."""
        if self._process is None:
            return False
        else:
            return self._process.is_alive()

    # Process
    def create_process(self, target=None, name=None, args=(), kwargs={}, daemon=None):
        """Creates a Process object to be stored within this object.

        Args:
            target: The function that will be executed by the separate process.
            name: The name of this object.
            args: The arguments for the function to be run in the separate process.
            kwargs: The keyword arguments for the function to be run the in the separate process.
            daemon (bool): Determines if the separate process will continue after the main process exits.
        """
        # Get previous attributes
        if self._process is not None:
            if target is None:
                try:
                    target = self._target
                except AttributeError:
                    pass
            if daemon is None:
                daemon = self.daemon
            if not args:
                args = self._args
            if not kwargs:
                kwargs = self._kwargs

        # Create new Process
        self._process = Process()

        # Set attributes after stashed attributes are set.
        if name is not None:
            self._process.name = name
        if target is not None:
            self._process._target = target
        if daemon is not None:
            self._process.daemon = daemon
        if args:
            self._process._args = args
        if kwargs:
            self._process._kwargs = kwargs

    def set_process(self, process):
        """Set this object's process to a new one.

        Args:
            process (:obj:`Process`): The new process.
        """
        self._process = process

    # Target
    def target_object_method(self, obj, method, args=(), kwargs={}):
        """Set the target to be a method of an object.

        Args:
            obj: The object the method will be executed from.
            method (str): The name of the method in the object.
            args: Arguments to be used by the method.
            kwargs: The keywords arguments to be used by the method.
        """
        kwargs["obj"] = obj
        kwargs["method"] = method
        self.create_process(target=self.method_wrapper, args=args, kwargs=kwargs)

    # Execution
    def start(self):
        """Starts running the process."""
        self.trace_log("separate_process", "start", "spawning new process...", name=self.name, level="DEBUG")
        try:
            self._process.start()
        except:
            self.create_process()
            self._process.start()

    def join(self, timeout=None):
        """Wait fpr the process to return/exit.

        Args:
            timeout (float, optional): The time in seconds to wait for the process to exit.
        """
        self._process.join(timeout)
        if self._process.exitcode is None:
            warnings.warn(TimeoutWarning("'join_async'"), stacklevel=2)

    async def join_async(self, timeout=None, interval=0.0):
        """Asynchronously, wait for the process to return/exit.

        Args:
            timeout (float, optional): The time in seconds to wait for the process to exit.
            interval (float, optional): The time in seconds between each queue query.
        """
        start_time = time.perf_counter()
        while self._process.exitcode is None:
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn(TimeoutWarning("'join_async'"), stacklevel=2)
                return

    def join_async_task(self, timeout=None, interval=0.0):
        """Creates an asyncio task which waits for the process to return/exit.

        Args:
            timeout (float): The time in seconds to wait for termination.
            interval (float): The time in seconds between termination checks. Zero means it will check ASAP.
        """
        return asyncio.create_task(self.join_async(timeout, interval))

    def restart(self):
        """Restarts the process."""
        if isinstance(self._process, Process):
            if self._process.is_alive():
                self._process.terminate()
        self.create_process()
        self._process.start()

    def close(self):
        """Closes the process and frees the resources."""
        if isinstance(self._process, Process):
            if self._process.is_alive():
                self._process.terminate()
            self._process.close()
