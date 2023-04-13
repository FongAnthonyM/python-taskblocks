""" iomanager.py
Extends ChainMap to hold Input and Output objects with methods to manage them.
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
from collections import ChainMap
from typing import Any

# Third-Party Packages #

# Local Packages #
from .interrupt import Interrupts


# Definitions #
# Classes #
class IOManager(ChainMap):
    """Extends ChainMap to hold Input and Output objects with methods to manage them.
    
    Attributes:
        interrupts: A container for interrupts.
        general: A container for uncategorized IO objects.
        pipes: A container for Pipes.
        queues: A container for Queues.
        events: A container for Events.
        managers: A container for Managers.
        
    Args:
        maps: Dictionary to append to the end of the chain map.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *maps: dict[str, Any]):
        # New Attributes #
        self.interrupts = Interrupts()

        m = [{}] * 5 + list(maps)
        self.general = m[0]
        self.pipes = m[1]
        self.queues = m[2]
        self.events = m[3]
        self.managers = m[4]

        # Construction #
        super().__init__(*m)

    # Instance Methods #
    def clear_all(self) -> None:
        """Clears all IO containers."""
        self.general.clear()
        self.events.clear()
        self.queues.clear()
        self.pipes.clear()
        self.managers.clear()
        
    def interrupt_all(self) -> None:
        """Interrupts all IO object."""
        for e in self.events.values():
            wait_interrupt = getattr(e, "wait_interrupt", None)
            hold_interrupt = getattr(e, "hold_interrupt", None)
            if wait_interrupt is not None:
                wait_interrupt.set()
            if hold_interrupt is not None:
                hold_interrupt.set()
        
        for q in self.queues.values():
            interrupt_all = getattr(q, "interrupt_all", None)
            get_interrupt = getattr(q, "get_interrupt", None)
            put_interrupt = getattr(q, "put_interrupt", None)
            if interrupt_all is not None:
                interrupt_all()
            if get_interrupt is not None:
                get_interrupt.set()
            if put_interrupt is not None:
                put_interrupt.set()

        for m in self.managers.values():
            interrupt_all = getattr(m, "interrupt_all", None)
            if interrupt_all is not None:
                interrupt_all()

    def uninterrupt_all(self) -> None:
        """Clears interrupts all in IO object."""
        for e in self.events.values():
            wait_interrupt = getattr(e, "wait_interrupt", None)
            hold_interrupt = getattr(e, "hold_interrupt", None)
            if wait_interrupt is not None:
                wait_interrupt.clear()
            if hold_interrupt is not None:
                hold_interrupt.clear()

        for q in self.queues.values():
            uninterrupt_all = getattr(q, "uninterrupt_all", None)
            get_interrupt = getattr(q, "get_interrupt", None)
            put_interrupt = getattr(q, "put_interrupt", None)
            if uninterrupt_all is not None:
                uninterrupt_all()
            if get_interrupt is not None:
                get_interrupt.clear()
            if put_interrupt is not None:
                put_interrupt.clear()

        for m in self.managers.values():
            uninterrupt_all = getattr(m, "uninterrupt_all", None)
            if uninterrupt_all is not None:
                uninterrupt_all()
