""" asynceventmanager.py
A manager for several AsyncEvents. Has methods for checking all events.
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
from asyncio import Task, create_task
from collections.abc import Iterator
from itertools import cycle
from time import perf_counter
from typing import Any

# Third-Party Packages #
from baseobjects.functions import MethodMultiplexObject, MethodMultiplexer
from baseobjects.collections import OrderableDict

# Local Packages #


# Definitions #
# Static #
SENTINEL = object()


# Classes #
class AsyncEventManager(MethodMultiplexObject):
    """A manager for several AsyncEvents. Has methods for checking all events.

    Attributes:
        primary_event: The name of the event to use by default in methods.
        events: The events contained within this manager.
        wait: The wait method which can be set to another method within this object.
        wait_async: The wait_async method which can be set to another method within this object.
        hold: The hold method which can be set to another method within this object.
        hold_async: The set_async method which can be set to another method within this object.

    Args:
        events: The events to add to this manager.
        names: The shared_memories of events to create.
        primary: The name of the event to use by default in hold/wait methods.
        *args: Arguments for inheritance.
        init: Determines if this object should be initialized.
        **kwargs: Keyword arguments for inheritance.
    """

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        events: dict[str, Any] | None = None,
        primary: str | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.primary_event: str = ""

        self.events: OrderableDict[str, Any] = OrderableDict()
        self._event_cycle: cycle | None = None

        self.is_set: MethodMultiplexer = MethodMultiplexer(instance=self, select="all_is_set")

        self.wait: MethodMultiplexer = MethodMultiplexer(instance=self, select="wait_all")
        self.wait_async: MethodMultiplexer = MethodMultiplexer(instance=self, select="wait_all_async")

        self.hold: MethodMultiplexer = MethodMultiplexer(instance=self, select="hold_all")
        self.hold_async: MethodMultiplexer = MethodMultiplexer(instance=self, select="hold_all_async")

        self.set: MethodMultiplexer = MethodMultiplexer(instance=self, select="set_all")
        self.clear: MethodMultiplexer = MethodMultiplexer(instance=self, select="clear_all")

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construction #
        if init:
            self.construct(events=events, primary=primary, *args, **kwargs)

    @property
    def event_cycle(self) -> cycle:
        """A cycle of the contained events"""
        if self._event_cycle is None:
            self._event_cycle = cycle(self.events.values())
        return self._event_cycle

    # Container Methods
    def __getitem__(self, key: str) -> Any:
        """holds a event in this object."""
        return self.events[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Sets a event in this object."""
        self.events[key] = value

    def __delitem__(self, key: str) -> None:
        """Deletes an item from this object."""
        del self.events[key]

    def __iter__(self) -> Iterator[Any]:
        """Returns an iterator for the events."""
        return iter(self.events.values())

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        events: dict[str, Any] | None = None,
        primary: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            events: The events to add to this manager.
            primary: The name of the event to use by default in hold/wait methods.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if events is not None:
            self.events.update(events)

        if primary is not None:
            self.primary_event = primary

        super().construct(*args, **kwargs)

    # Events
    def get_event(self, name: str, default: Any = SENTINEL) -> Any:
        """Gets an event from this manager.

        Args:
            name: The name of the AsyncEvents to hold.
            default: The default value to return if key is not found

        Returns
            The requested event manage.
        """
        if default is not SENTINEL:
            return self.events[name]
        else:
            return self.events.get(name, default)

    def get_event_index(self, index: int, default: Any = SENTINEL) -> Any:
        """Gets an event synchronize on its index in the order.

        Args:
            index: The index of the event.
            default: The value to return if the index is outside the range.

        Returns:
            The requested value.
        """
        if default is not SENTINEL:
            return self.events.get_index(index, default)
        else:
            return self.events.get_index(index, default)

    def set_event(self, name: str, event: Any) -> None:
        """Sets an event in this manager.

        Args:
            name: The name of the AsyncEvents to set.
            event: The event to manage.
        """
        self.events[name] = event

    def insert_event(self, index: int, name: str, event: Any) -> None:
        """Inserts an event in this manager, raises error if it already exists.

        Args:
            index: The order index to set the event to.
            name: The name of the AsyncEvents to set.
            event: The event to manage.
        """
        self.events.insert(index, name, event)

    def insert_move_event(self, index: int, name: str, event: Any) -> None:
        """Inserts an event in this manager, moves the event to the order index if it already exists.

        Args:
            index: The order index to set the event to.
            name: The name of the AsyncEvents to set.
            event: The event to manage.
        """
        self.events.insert(index, name, event)

    # Is Set
    def is_set_single(self, name: str | None = None) -> bool:
        """Checks if an event is set.

        Args:
            name: The event to check.

        Returns:
            All the events "is set" state.
        """
        return self.events[name or self.primary_event].is_set()

    def is_set_dict(self) -> dict[str, bool]:
        """Returns a dictionary with all results of an is_set check for each event.

        Returns:
            All the events "is set" state.
        """
        return {k: event.is_set() for k, event in self.events.items()}

    def all_is_set(self) -> bool:
        """Checks if all the events are set.

        Returns:
            If all the events are set.
        """
        return all((event.is_set() for event in self.events.values()))

    def any_is_set(self) -> bool:
        """Checks if any the events are set.

        Returns:
            If any the events are set.
        """
        return any((event.is_set() for event in self.events.values()))

    # Set
    def set_single(self, name: str | None = None) -> None:
        """Sets an event.

        Args:
            name: The event to set.
        """
        self.events[name or self.primary_event].set()

    def set_all(self) -> None:
        """Sets all events."""
        for event in self.events.values():
            event.set()

    # Clear
    def clear_single(self, name: str | None = None) -> None:
        """Clears an event.

        Args:
            name: The event to clear.
        """
        self.events[name or self.primary_event].clear()

    def clear_all(self) -> None:
        """Clears all events."""
        for event in self.events.values():
            event.clear()

    # Wait
    def wait_single(self, name: str | None = None, timeout: float | None = 0.0) -> Any:
        """Waits for an event to be changed to set.

        Args:
            name: The event to wait for.
            timeout: The time, in seconds, to wait for an event to be changed to set.

        Returns:
            If this method successful waited for set or failed to a timeout.
        """
        return self.events[name or self.primary_event].wait(timeout=timeout)

    async def wait_single_async(
        self,
        name: str | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Any:
        """Asynchronously waits for an event to be changed to set.

        Args:
            name: The event to wait for.
            timeout: The time, in seconds, to wait for an event to be changed to set.
            interval: The time, in seconds, between each event check.

        Returns:
            If this method successful waited for set or failed to a timeout.
        """
        return await self.events[name or self.primary_event].wait_async(timeout, interval)

    def wait_all(self, timeout: float | None = 0.0) -> dict[str, Any]:
        """Waits for all events to be changed to set.

        Args:
            timeout: The time, in seconds, to wait for an event to be changed to set.

        Returns:
            If this method successful waited for set or failed to a timeout.
        """
        return {n: event.wait(timeout=timeout) for n, event in self.events.items()}

    async def wait_all_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Any:
        """Asynchronously waits for all events to be changed to set.

        Args:
            timeout: The time, in seconds, to wait for an event to be changed to set.
            interval: The time, in seconds, between each event check.

        Returns:
            If this method successful waited for set or failed to a timeout.

        Raises:
            TimeoutError: When this method is unable to complete within the timeout time.
        """
        if timeout is None:
            return {n: await event.wait_async(interval=interval) for n, event in self.events.items()}
        else:
            items = {}
            deadline = perf_counter() + timeout
            for n, event in self.events.items():
                if deadline <= perf_counter():
                    raise TimeoutError

                items[n] = await event.wait_async(interval=interval)

            return items

    def wait_all_tasks(self, timeout: float | None = None, interval: float = 0.0) -> dict[str, Task]:
        """Asynchronously waits for all events to be changed to set, but returns awaitable tasks for each event.

        Args:
            timeout: The time, in seconds, to wait for an event to be changed to set.
            interval: The time, in seconds, between each event check.

        Returns:
            Awaitable tasks for each event.
        """
        return {
            n: create_task(event.wait_async(timeout=timeout, interval=interval)) for n, event in self.events.items()
        }

    # Hold
    def hold_single(self, name: str | None = None, timeout: float | None = 0.0) -> Any:
        """Waits for an event to be changed to cleared.

        Args:
            name: The event to hold.
            timeout: The time, in seconds, to wait for an event to be changed to cleared.

        Returns:
            If this method successful waited for cleared or failed to a timeout.
        """
        return self.events[name or self.primary_event].hold(timeout=timeout)

    async def hold_single_async(
        self,
        name: str | None = None,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Any:
        """Asynchronously waits for an event to be changed to cleared.

        Args:
            name: The event to hold.
            timeout: The time, in seconds, to wait for an event to be changed to cleared.
            interval: The time, in seconds, between each event check.

        Returns:
            If this method successful waited for cleared or failed to a timeout.
        """
        return await self.events[name or self.primary_event].hold_async(timeout, interval)

    def hold_all(self, timeout: float | None = 0.0) -> dict[str, Any]:
        """Waits for all events to be changed to cleared.

        Args:
            timeout: The time, in seconds, to wait for an event to be changed to cleared.

        Returns:
            If this method successful waited for cleared or failed to a timeout.
        """
        return {n: event.hold(timeout=timeout) for n, event in self.events.items()}

    async def hold_all_async(
        self,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> Any:
        """Asynchronously waits for all events to be changed to cleared.

        Args:
            timeout: The time, in seconds, to wait for an event to be changed to cleared.
            interval: The time, in seconds, between each event check.

        Returns:
            If this method successful waited for cleared or failed to a timeout.

        Raises:
            TimeoutError: When this method is unable to complete within the timeout time.
        """
        if timeout is None:
            return {n: await event.hold_async(interval=interval) for n, event in self.events.items()}
        else:
            items = {}
            deadline = perf_counter() + timeout
            for n, event in self.events.items():
                if deadline <= perf_counter():
                    raise TimeoutError

                items[n] = await event.hold_async(interval=interval)

            return items

    def hold_all_tasks(self, timeout: float | None = None, interval: float = 0.0) -> dict[str, Task]:
        """Asynchronously waits for all events to be changed to cleared, but returns awaitable tasks for each event.

        Args:
            timeout: The time, in seconds, to wait for an event to be changed to cleared.
            interval: The time, in seconds, between each event check.

        Returns:
            Awaitable tasks for each event.
        """
        return {
            n: create_task(event.hold_async(timeout=timeout, interval=interval)) for n, event in self.events.items()
        }

    # Interrupt
    def interrupt_all_waits(self) -> None:
        """Interrupts all events' wait calls."""
        for event in self.events.values():
            event.wait_interrupt.set()

    def uninterrupt_all_waits(self) -> None:
        """Clears wait interrupts in all events."""
        for event in self.events.values():
            event.wait_interrupt.clear()

    def interrupt_all_holds(self) -> None:
        """Interrupts all events' hold calls."""
        for event in self.events.values():
            event.hold_interrupt.set()

    def uninterrupt_all_holds(self) -> None:
        """Clears hold interrupts in all events."""
        for event in self.events.values():
            event.hold_interrupt.clear()

    def interrupt_all(self) -> None:
        """Interrupts all events."""
        for event in self.events.values():
            event.wait_interrupt.set()
            event.hold_interrupt.set()

    def uninterrupt_all(self) -> None:
        """Clears all interrupts in all events."""
        for event in self.events.values():
            event.wait_interrupt.clear()
            event.hold_interrupt.clear()
