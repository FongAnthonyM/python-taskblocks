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
from advancedlogging import AdvancedLogger

# Local Libraries #
from .task import Task


# Definitions #
# Classes #
# Tasks #
@dataclasses.dataclass
class ListenerInfoUnit:
    """A Dataclass that hold information about listeners for the log listener."""
    name: str
    queue: Queue
    handlers: list
    respect_handler_level: False


# Todo: Create LogListener Class or Edit the existing one.


class LogListener(Task):
    class_loggers = {"log_listener_root": AdvancedLogger("log_listener_root")}

    def __init__(self, q_handlers=None, name="log_listener", init=True):
        super().__init__(init=False)
        self.listeners = {}

        if init:
            self.construct(q_handlers, name)

    # Constructors/Destructors
    def construct(self, q_handlers=None, name=None, **kwargs):
        """Constructs this object

        Args:
            q_handlers: The queue and handlers to
            name (str, optional): Name of this object.
            kwargs: The keyword arguments for the Task constructor.
        """
        super().construct(name=name, **kwargs)
        if q_handlers is not None:
            self.update(q_handlers)

    # Container Methods
    def keys(self):
        """Gets the keys of the listeners.

        Returns:
            The keys of the listeners.
        """
        return self.listeners.keys()

    def values(self):
        """Get all the listeners within this object.

        Returns:
            All of the listeners within this object.
        """
        return self.listeners.values()

    def items(self):
        """Gets the items, name and unit, within this object.

        Returns:
            The listeners and their names within this object
        """
        return self.listeners.items()

    def set_listener(self, name, queue, *handlers, respect_handler_level=False):
        """Sets a single unit within this object.

        Args:
            name: The name of the unit.
            queue: The queue to get the LogRecords from.
            handlers: The handlers that will process the LogRecords.
            respect_handler_level: Determines if the log level will checked again for each handler.
        """
        self.listeners[name] = ListenerInfoUnit(name, queue, handlers, respect_handler_level)

    def update(self, listeners):
        """Updates the contained unit dictionary with the new dictionary.

        Args:
            listeners (:obj:`dict` of :obj:`Task`): The new dictionary to add to the listeners of this object.
        """
        for name, unit in listeners.items():
            if isinstance(unit, ListenerInfoUnit):
                self.listeners[name] = unit
            elif isinstance(unit, dict):
                if "name" not in unit:
                    unit["name"] = name
                self.listeners[name] = ListenerInfoUnit(**unit)

    def pop(self, name):
        """Removes the named unit from this object.

        Args:
            name: The name of the tasks unit to remove from this object.

        Returns:
            A
        """
        return self.listeners.pop(name)

    def clear(self):
        """Clears the task listeners from this object."""
        self.listeners.clear()

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

    # Closure
    def closure(self, **kwargs):
        """The method to run after executing task."""
        self.trace_log("task_root", "closure", "closure method not overridden", name=self.name, level="DEBUG")
