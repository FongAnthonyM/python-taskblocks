""" provxyprocess.py
A proxy for a Process which acts like a Process, but can run more than once.
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
from asyncio import sleep
from multiprocessing import Process, cpu_count
from time import perf_counter
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject, search_sentinel

# Local Packages #
from ..io import Interrupt


# Definitions #
# Classes #
class ProcessProxy(BaseObject):
    """A proxy for a Process which acts like a Process, but can run more than once.

    ProcessProxy is a proxy for Process instance where the process target and specifications are held in ProcessProxy
    then distributed to a new Process when the ProcessProxy is started. This way ProcessProxy runs the process target
    multiple times by creating identical Processes and running them.

    Class Attributes:
        CPU_COUNT: The number of CPUs this computer has.

    Attributes:
        target: The function that will be executed by the separate process.
        name: The name of this object.
        args: The arguments for the function to be run in the separate process.
        kwargs: The keyword arguments for the function to be run the in the separate process.
        daemon: Determines if the separate process will continue after the main process exits.
        join_interrupt: An event which can be used to interrupt the join method.
        process: The Process object to run.
        previous_processes: The previous Process objects used in previous runs.

    Args:
        target: The function that will be executed by the separate process.
        name: The name of this object.
        args: The arguments for the function to be run in the separate process.
        kwargs: The keyword arguments for the function to be run the in the separate process.
        daemon : Determines if the separate process will continue after the main process exits.
        init: Determines if this object will construct.
    """
    CPU_COUNT: int = cpu_count()

    # Construction/Destruction
    def __init__(
        self,
        group: None = None,
        target: None = None,
        name: str | None | object = search_sentinel,
        args: tuple[Any] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        daemon: bool | None = None,
        init=True,
    ) -> None:
        # New Attributes #
        self.target: Any | None = None
        self.name: str | None = None
        self.args: tuple[Any] = tuple()
        self.kwargs: dict[str, Any] = {}
        self.daemon: bool | None = None

        self.join_interrupt: Interrupt | None = None

        self.process: Process | None = None
        self.previous_processes: list[Process] = []

        # Parent Attributes #
        super().__init__()

        # Construct #
        if init:
            self.construct(target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)

    @property
    def authkey(self) -> str | None:
        """Authorization key of the process."""
        return None if self.process is None else self.process.authkey

    @authkey.setter
    def authkey(self, authkey: str) -> None:
        self.process.authkey = authkey

    @property
    def exitcode(self) -> int:
        """Return exit code of process or `None` if it has yet to stop."""
        try:
            return self.process.exitcode
        except AttributeError:
            raise ValueError("The process has not been started.")

    @property
    def ident(self) -> int | None:
        """Return identifier (PID) of process or `None` if it has yet to start"""
        try:
            return self.process.ident
        except AttributeError:
            return None

    pid = ident

    @property
    def sentinel(self):
        """Return a file descriptor (Unix) or handle (Windows) suitable for waiting for process termination."""
        try:
            return self.process.sentinel
        except AttributeError:
            raise ValueError("process not started") from None

    # Pickling
    def __getstate__(self) -> dict[str, Any]:
        """Creates a dictionary of attributes which can be used to rebuild this object.

        Returns:
            dict: A dictionary of this object's attributes.
        """
        out_dict = self.__dict__.copy()
        out_dict["_process"] = None
        return out_dict

    # Representation
    def __repr__(self) -> str:
        """The string representation of this object."""
        return super().__repr__() if self.process is None else self.process.__repr__()

    # Constructors
    def construct(
        self,
        target: None = None,
        name: str | None | object = search_sentinel,
        args: tuple[Any] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        """Constructs this object.

        Args:
            target: The function that will be executed by the separate process.
            name: The name of this object.
            args: The arguments for the function to be run in the separate process.
            kwargs: The keyword arguments for the function to be run the in the separate process.
            daemon: Determines if the separate process will continue after the main process exits.
        """
        if target is not None:
            self.target = target

        if name is not search_sentinel:
            self.name = name

        if args:
            self.args = args

        if kwargs is not None:
            self.kwargs = kwargs

        if daemon is not None:
            self.daemon = daemon

    # State
    def is_alive(self) -> bool:
        """Checks if the process is running.

        Returns:
            If the current process is running.
        """
        return False if self.process is None else self.process.is_alive()

    # Process
    def _create_process(self) -> None:
        """Creates a Process object to be stored within this object."""
        target = self.target or self.run
        name = None if self.name is None else self.name.join(f"_proxy{len(self.previous_processes)}")

        self.process = Process(target=target, name=name, args=self.args, kwargs=self.kwargs, daemon=self.daemon)

    def create_process(
        self,
        target: None = None,
        name: str | None | object = search_sentinel,
        args: tuple[Any] = (),
        kwargs: dict[str, Any] | None = None,
        daemon: bool | None = None,
    ) -> None:
        """Creates a Process object to be stored within this object.

        Args:
            target: The function that will be executed by the separate process.
            name: The name of this object.
            args: The arguments for the function to be run in the separate process.
            kwargs: The keyword arguments for the function to be run the in the separate process.
            daemon: Determines if the separate process will continue after the main process exits.
        """
        if target is not None:
            self.target = target

        if name is not search_sentinel:
            self.name = name

        if args:
            self.args = args

        if kwargs is not None:
            self.kwargs = kwargs

        if daemon is not None:
            self.daemon = daemon

        self._create_process()

    # Target
    def run(self, *args: Any, **kwargs) -> None:
        """Method to be run in sub-process; can be overridden in sub-class"""
        if self.target:
            self.target(*args, **kwargs)

    def start(self) -> None:
        """Starts running the process."""
        if self.process is None:
            self._create_process()
        elif self.process._closed:
            self.previous_processes.append(self.process)
            self._create_process()
        elif self.process.exitcode is not None:
            self.process.close()
            self.previous_processes.append(self.process)
            self._create_process()
        elif self.process.is_alive():
            raise RuntimeError(f"{self} process is already running.")

        self.process.start()

    def terminate(self) -> None:
        """Terminate process; sends SIGTERM signal or uses TerminateProcess()"""
        try:
            self.process.terminate()
        except (AttributeError, ValueError):
            raise ValueError("Process is not alive to terminate")

    def kill(self) -> None:
        """Terminate process; sends SIGKILL signal or uses TerminateProcess()"""
        try:
            self.process.kill()
        except (AttributeError, ValueError):
            raise ValueError("Process is not alive to kill")

    def join(self, timeout: float | None = None) -> None:
        """Wait until child process terminates.

        Args:
            timeout: The time, in seconds, to wait for the process to exit.
        """
        assert self.process is not None, 'can only join a started process'
        self.process.join(timeout)

    async def join_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
        interrupt: Interrupt | None = None,
    ) -> None:
        """Asynchronously, wait for the process to return/exit.

        Args:
            timeout: The time, in seconds, to wait for the process to exit.
            interval: The time, in seconds, between each join check.
            interrupt: A interrupt which can be used to escape the loop.
        """
        self.join_interrupt = interrupt or Interrupt()
        if timeout is None:
            while self.process.exitcode is None:
                if self.join_interrupt.is_set():
                    raise InterruptedError
                await sleep(interval)
        else:
            deadline = perf_counter() + timeout
            while self.process.exitcode is None:
                if self.join_interrupt.is_set():
                    raise InterruptedError
                if deadline <= perf_counter():
                    raise TimeoutError
                await sleep(interval)

    def close(self) -> None:
        """Closes the process and frees the resources."""
        try:
            self.process.close()
        except AttributeError:
            raise ValueError("No process close")
