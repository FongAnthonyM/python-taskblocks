""" simplexpipemanager.py

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
import asyncio
from collections.abc import Iterable
from multiprocessing.connection import Connection, Pipe
from multiprocessing.reduction import ForkingPickler
from queue import Empty
import time
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject

# Local Packages #
from .interrupt import Interrupt


# Definitions #


# Classes #
class SimplexPipeManager(BaseObject):
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
        self.recv_interrupt: Interrupt | None = None

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
            names: Names of pipes to create.
            duplex: Determines if the created pipes will be duplexes.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if names is not None:
            self.create_pipes(names=names, duplex=duplex)

        super().construct(*args, **kwargs)

    # Pipe
    def create_pipe(self, name: str, duplex: bool = False) -> tuple[Connection, Connection]:
        self.recv_connections[name], self.send_connections[name] = Pipe(duplex=duplex)
        return self.send_connections[name], self.recv_connections[name]

    def create_pipes(self, names: Iterable[str] = (), duplex: bool = False) -> None:
        for name in names:
            self.recv_connections[name], self.send_connections[name] = Pipe(duplex=duplex)

    def set_connection(self, name: str, recv: Connection, send: Connection) -> None:
        self.recv_connections[name] = recv
        self.send_connections[name] = send

    def get_connection(self, name: str) -> tuple[Connection, Connection]:
        return self.recv_connections[name], self.send_connections[name]

    # Object Query
    def poll(self, name: str, timeout: float | None = 0.0) -> bool:
        return self.recv_connections[name].poll(timeout)

    def poll_all(self, timeout: float | None = 0.0) -> dict[str, bool]:
        return {n: c.poll(timeout) for n, c in self.recv_connections.items()}

    def all_empty(self) -> bool:
        for connection in self.recv_connections.values():
            if not connection.poll():
                return False
        return True

    def any_empty(self) -> bool:
        for connection in self.recv_connections.values():
            if connection.poll():
                return True
        return False

    # Receive
    def recv_bytes(
        self,
        name: str,
        maxlength: int | None = None,
        block: bool = True,
        timeout: float | None = None,
    ) -> bytes:
        connection = self.recv_connections[name]
        if not block and not connection.poll():
            raise Empty
        elif block and timeout is not None:
            deadline = time.perf_counter() + timeout
            while not connection.poll():
                if deadline <= time.perf_counter():
                    raise Empty

        return connection.recv_bytes(maxlength)

    async def recv_bytes_async(
        self,
        name: str,
        maxlength: int | None = None,
        timeout: float | None = None,
        interval: bool = 0.0,
        interrupt: Interrupt | None = None,
    ) -> bytes:
        self.recv_interrupt = Interrupt() if interrupt is None else interrupt
        connection = self.recv_connections[name]
        deadline = None if timeout is None else time.perf_counter() + timeout
        while not connection.poll():
            await asyncio.sleep(interval)
            if deadline is not None and deadline <= time.perf_counter():
                raise Empty
            if self.recv_interrupt.is_set():
                raise InterruptedError

        return connection.recv_bytes(maxlength)

    def recv(
        self,
        name: str,
        block: bool = True,
        timeout: float | None = None,
    ) -> Any:
        connection = self.recv_connections[name]
        if not block and not connection.poll():
            raise Empty
        elif block and timeout is not None:
            deadline = time.perf_counter() + timeout
            while not connection.poll():
                if deadline <= time.perf_counter():
                    raise Empty

        return connection.recv()

    async def recv_async(
        self,
        name: str,
        timeout: float | None = None,
        interval: bool = 0.0,
        interrupt: Interrupt | None = None,
    ) -> Any:
        self.recv_interrupt = Interrupt() if interrupt is None else interrupt
        connection = self.recv_connections[name]
        deadline = None if timeout is None else time.perf_counter() + timeout
        while not connection.poll():
            await asyncio.sleep(interval)
            if deadline is not None and deadline <= time.perf_counter():
                raise Empty
            if self.recv_interrupt.is_set():
                raise InterruptedError

        return connection.recv()

    def clear_recv(self, name: str) -> None:
        connection = self.recv_connections[name]
        while connection.poll:
            connection.recv()

    # Send
    def send_bytes(self, name: str, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        self.send_connections[name].send_bytes(buf, offset, size)

    def send_bytes_all(self, buf: bytes, offset: int = 0, size: int | None = None) -> None:
        for connection in self.send_connections.values():
            connection.send_bytes(buf, offset, size)

    def send(self, name: str, obj: Any) -> None:
        self.send_bytes(name, ForkingPickler.dumps(obj))

    def send_all(self, obj: Any) -> None:
        self.send_bytes_all(ForkingPickler.dumps(obj))
