""" asyncqueueinterface.py
An interface which outlines the basis for an async queue.
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
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject

# Local Packages #


# Definitions #
# Classes #
class AsyncQueueInterface(BaseObject):
    """An interface which outlines the basis for an async queue."""

    # Instance Methods #
    # Queue
    def empty(self) -> bool:
        """Determines if this queue is empty."""
        raise NotImplemented

    def get(self, block: bool = True, timeout: float | None = None, *args: Any, **kwargs: Any) -> Any:
        """Gets an item from the queue."""
        raise NotImplemented

    async def get_async(
        self, block: bool = True, timeout: float | None = None, interval: float = 0.0, *args: Any, **kwargs: Any
    ) -> Any:
        """Asynchronously gets an item from the queue."""
        raise NotImplemented

    def put(self, *args: Any, **kwargs: Any) -> None:
        """Puts an item from on the queue."""
        raise NotImplemented

    async def put_async(self, timeout: float | None = None, interval: float = 0.0, *args: Any, **kwargs: Any) -> None:
        """Asynchronously puts an item from on the queue."""
        raise NotImplemented

    def join(self) -> None:
        """Blocks until all items in the Queue have been gotten."""
        pass

    async def join_registry_async(self, interval: float = 0.0) -> None:
        """Asynchronously, blocks until all items in the Queue have been gotten.

        Args:
            interval: The time, in seconds, between each queue check.
        """
        pass
