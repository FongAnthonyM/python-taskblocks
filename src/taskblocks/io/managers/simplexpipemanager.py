""" simplexpipemanager.py
A manager for several multiprocessing Pipes. Has methods for sending and receiving data on all pipes.
"""
# Package Header #
from ...header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from asyncio import sleep
from collections.abc import Iterable
from multiprocessing.connection import Connection, Pipe
from multiprocessing.reduction import ForkingPickler
from queue import Empty
from time import perf_counter
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject

# Local Packages #
from ..synchronize import Interrupt


# Definitions #


# Classes #
class SimplexPipeManager(BaseObject):
    """A manager for several multiprocessing Pipes. Has methods for sending and receiving data on all pipes.

    Attributes:
        send_connections: The send connections within this manager.
        recv_connections: The receive connections within this manager.

    Args:
        names: The names of pipes to create.
        duplex: Determines if the created pipes will be duplexes.
        *args: Arguments for inheritance.
        init: Determines if this object should be initialized.
        **kwargs: Keyword arguments for inheritance.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        names: Iterable[str] | None = None,
        duplex: bool = False,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.recv_interrupt: Interrupt = Interrupt()

        self.send_connections: dict[str, Connection] = {}
        self.recv_connections: dict[str, Connection] = {}

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construction #
        if init:
            self.construct(names=names, duplex=duplex, *args, **kwargs)

    # Instance Methods #
    # Constructors/Destructors
    def construct(self, names: Iterable[str] | None = None, duplex: bool = False, *args: Any, **kwargs: Any) -> None:
        """Constructs this object.

        Args:
            names: The names of pipes to create.
            duplex: Determines if the created pipes will be duplexes.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if names is not None:
            self.create_pipes(names=names, duplex=duplex)

        super().construct(*args, **kwargs)

    # Pipe
    def create_pipe(self, name: str, duplex: bool = False) -> tuple[Connection, Connection]:
        """Creates a Pipe to manage.

        Args:
            name: The name of the pipe to create.
            duplex: Determines if the pipe will be a duplex.

        Returns:
            The receive and send connections of the Pipe.
        """
        self.recv_connections[name], self.send_connections[name] = Pipe(duplex=duplex)
        return self.send_connections[name], self.recv_connections[name]

    def create_pipes(self, names: Iterable[str] = (), duplex: bool = False) -> None:
        """Creates Pipes to manage.

        Args:
            names: The names of the pipes to create.
            duplex: Determines if the pipes will be duplexes.
        """
        for name in names:
            self.recv_connections[name], self.send_connections[name] = Pipe(duplex=duplex)

    def set_connection(self, name: str, recv: Connection, send: Connection) -> None:
        """Sets a connection pair to manage.

        Args:
            name: The names of the connection to manage.
            recv: The receive connection to manage.
            send: The send connection to manage.
        """
        self.recv_connections[name] = recv
        self.send_connections[name] = send

    def get_connection(self, name: str) -> tuple[Connection, Connection]:
        """Gets a connection pair.

        Args:
            name: The names of the connection to manage.
            
        Returns
            The requested receive and send connections.
        """
        return self.recv_connections[name], self.send_connections[name]

    # Object Query
    def poll(self, name: str, timeout: float | None = 0.0) -> bool:
        """Polls the named receive connection.
        
        Args:
            name: The name of the receive connection to poll.
            timeout: The time, in seconds, to wait for the connection poll.
        
        Returns:
            If there is something in the connection.
        """
        return self.recv_connections[name].poll(timeout)

    def poll_all(self, timeout: float | None = 0.0) -> dict[str, bool]:
        """Polls all receive connections.

        Args:
            timeout: The time, in seconds, to wait for the connection poll.
        
        Returns:
            All poll results for each receive connection.
        """
        return {n: c.poll(timeout) for n, c in self.recv_connections.items()}

    def empty(self) -> dict[str, bool]:
        """Returns a dictionary with all results of an empty check for each receive connection.

        Returns:
            All the receive connections "is empty" state.
        """
        return {k: not q.poll() for k, q in self.recv_connections.items()}

    def all_empty(self) -> bool:
        """Checks if all the recv_connections are empty.

        Returns:
            If all the receive connections are empty.
        """
        return all((not q.poll() for q in self.recv_connections.values()))

    def any_empty(self) -> bool:
        """Checks if any the receive connections are empty.

        Returns:
            If any the receive connections are empty.
        """
        return any((not q.poll() for q in self.recv_connections.values()))

    # Receive
    def wait_recv(
        self,
        name: str,
        block: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Waits for the poll of a receive connection to be true.

        Args:
            name: The receive connection to get the bytes from.
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for the bytes.

        Raises:
            Empty: When there are no items to get in the queue when not blocking or on timing out.
            InterruptedError: When this method is interrupted by an interrupt event.
        """
        connection = self.recv_connections[name]
        if not block and not connection.poll():
            raise Empty
        elif block and timeout is not None:
            deadline = perf_counter() + timeout
            while not connection.poll():
                if deadline <= perf_counter():
                    raise Empty
                if self.recv_interrupt.is_set():
                    raise InterruptedError

    async def wait_recv_async(
        self,
        name: str,
        timeout: float | None = None,
        interval: bool = 0.0,
    ) -> None:
        """Asynchronously waits for the poll of a receive connection to be true.

        Args:
            name: The receive connection to get the bytes from.
            timeout: The time, in seconds, to wait for the bytes.
            interval: The time, in seconds, between each poll check.

        Returns:
            The requested bytes.
        """
        connection = self.recv_connections[name]
        if timeout is None:
            while not connection.poll():
                await sleep(interval)
                if self.recv_interrupt.is_set():
                    raise InterruptedError
        else:
            deadline = perf_counter() + timeout
            while not connection.poll():
                await sleep(interval)
                if deadline <= perf_counter():
                    raise Empty
                if self.recv_interrupt.is_set():
                    raise InterruptedError

    def recv_bytes(
        self,
        name: str,
        maxlength: int | None = None,
        block: bool = True,
        timeout: float | None = None,
    ) -> bytes:
        """Receives the bytes from a receive connection.

        Args:
            name: The receive connection to get the bytes from.
            maxlength: The max length the receive bytes can be.
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for the bytes.

        Returns:
            The requested bytes.
        """
        self.wait_recv(name=name, block=block, timeout=timeout)
        return self.recv_connections[name].recv_bytes(maxlength)

    async def recv_bytes_async(
        self,
        name: str,
        maxlength: int | None = None,
        timeout: float | None = None,
        interval: bool = 0.0,
    ) -> bytes:
        """Asynchronously receives the bytes from a receive connection.

        Args:
            name: The receive connection to get the bytes from.
            maxlength: The max length the receive bytes can be.
            timeout: The time, in seconds, to wait for the bytes.
            interval: The time, in seconds, between each poll check.

        Returns:
            The requested bytes.
        """
        await self.wait_recv_async(name=name, timeout=timeout, interval=interval)
        return self.recv_connections[name].recv_bytes(maxlength)

    def recv(
        self,
        name: str,
        block: bool = True,
        timeout: float | None = None,
    ) -> Any:
        """Receives an item from a receive connection.

        Args:
            name: The receive connection to get the item from.
            block: Determines if this method will block execution.
            timeout: The time, in seconds, to wait for the item.

        Returns:
            The requested item.
        """
        self.wait_recv(name=name, block=block, timeout=timeout)
        return self.recv_connections[name].recv()

    async def recv_async(
        self,
        name: str,
        timeout: float | None = None,
        interval: bool = 0.0,
    ) -> Any:
        """Asynchronously receives an item from a receive connection.

        Args:
            name: The receive connection to get the item from.
            timeout: The time, in seconds, to wait for the item.
            interval: The time, in seconds, between each poll check.

        Returns:
            The requested item.
        """
        await self.wait_recv_async(name=name, timeout=timeout, interval=interval)
        return self.recv_connections[name].recv()

    def clear_recv(self, name: str) -> None:
        """Removes all items in a receive connection.

        Args:
            name: The receive connection to clear.
        """
        connection = self.recv_connections[name]
        while connection.poll:
            connection.recv()

    # Send
    def send_bytes(self, name: str, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        """Sends bytes to a send connection.

        Args:
            name: The send connection to send the bytes to.
            buf: The bytes buffer to send into the send connection.
            offset: The offset in the buffer to send the bytes into the send connection.
            size: The amount of the bytes into the send connection.
        """
        self.send_connections[name].send_bytes(buf, offset, size)

    def send_bytes_all(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        """Sends bytes to all send connections.

        Args:
            buf: The bytes buffer to send into each send connection.
            offset: The offset in the buffer to send the bytes into each send connection.
            size: The amount of the bytes into each send connection.
        """
        for connection in self.send_connections.values():
            connection.send_bytes(buf, offset, size)

    def send(self, name: str, obj: Any) -> None:
        """Sends an object to a send connection.

        Args:
            name: The send connection to send the object to.
            obj: The object to send into the send connection.
        """
        self.send_bytes(name, ForkingPickler.dumps(obj))

    def send_all(self, obj: Any) -> None:
        """Sends an object to all send connection.

        Args:
            obj: The object to send into each send connection.
        """
        self.send_bytes_all(ForkingPickler.dumps(obj))

    # Interrupt
    def interrupt_all(self) -> None:
        """Interrupts all receive methods."""
        self.recv_interrupt.set()

    def uninterrupt_all(self) -> None:
        """Clears all interrupts in all receive methods."""
        self.recv_interrupt.clear()
