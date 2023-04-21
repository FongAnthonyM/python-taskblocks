""" interrupt.py
An AsyncEvent which intended to act interrupt.
"""
# Package Header #
from ....header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Standard Libraries #
from multiprocessing.synchronize import Event
from multiprocessing.context import BaseContext

# Third-Party Packages #

# Local Packages #
from ..asyncevent import AsyncEvent


# Definitions #
# Classes #
class Interrupt(AsyncEvent):
    """An AsyncEvent which intended to act interrupt.

    Attributes:
        parent: An Event which, if set, will also set this interrupt.

    Args:
        parent: An Event which, if set, will also set this interrupt.
        ctx: The context for the Python multiprocessing.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, parent: Event | None = None, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.parent: Event | None = parent

        # Construction #
        super().__init__(ctx=ctx)

    # Instance Methods #
    def is_set(self) -> bool:
        """Checks if this interrupt or its parent has been set.

        Returns:
            If this interrupt or its parent has been set.
        """
        if self.parent is not None and self.parent.is_set():
            self.set()
        return super().is_set()
