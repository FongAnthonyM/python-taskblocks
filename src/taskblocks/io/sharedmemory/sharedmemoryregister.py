"""sharedmemoryregister.py
A register of SharedMemory shared_memories which can also unlink them as needed.
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
from collections.abc import Iterable
from multiprocessing.shared_memory import SharedMemory
from typing import Any

# Third-Party Packages #
from baseobjects import BaseObject

# Local Packages #


# Definitions #
# Classes #
class SharedMemoryRegister(BaseObject):
    """A register of SharedMemory names which can also unlink them as needed.

    Attributes:
        shared_memories: The names of the SharedMemory in the register.

    Args:
        sms: The SharedMemory to register.
        *args: Arguments for inheritance.
        init: Determines if this object should be initialized.
        **kwargs: Keyword arguments for inheritance.
    """

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        sms: Iterable[str, ...] | Iterable[SharedMemory, ...] | None = None,
        *args: Any,
        init: bool = True,
        **kwargs: Any,
    ) -> None:
        # New Attributes #
        self.shared_memories: dict[str, SharedMemory] = {}

        # Parent Attributes #
        super().__init__(*args, init=False, **kwargs)

        # Construction #
        if init:
            self.construct(sms=sms, *args, **kwargs)

    def __del__(self) -> None:
        """Unlink all SharedMemory when this register is deleted."""
        self.unlink_all()

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        sms: Iterable[str, ...] | Iterable[SharedMemory, ...] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Constructs this object.

        Args:
            sms: The SharedMemory to register.
            *args: Arguments for inheritance.
            **kwargs: Keyword arguments for inheritance.
        """
        if sms is not None:
            for sm in sms:
                self.register_shared_memory(sm)

        super().construct(*args, **kwargs)

    def register_shared_memory(self, sms: SharedMemory | str) -> None:
        """Registers the supplied SharedMemory to this register.

        Args:
            sms: The SharedMemory or its name to add to the register.
        """
        if isinstance(sms, str):
            sms = SharedMemory(name=sms)

        self.shared_memories[sms.name] = sms

    def deregister_shared_memory(self, sms: SharedMemory | str) -> None:
        """Deregisters the supplied SharedMemory to this register.

        Args:
           sms: The SharedMemory or its name to remove from the register.
        """
        del self.shared_memories[sms if isinstance(sms, str) else sms.name]

    def unlink(self, name: str) -> None:
        """Unlinks a SharedMemory in this register.

        Args:
            name: The name of the SharedMemory to unlink.
        """
        segment = self.shared_memories.pop(name)
        segment.close()
        segment.unlink()

    def unlink_all(self) -> None:
        """Unlinks all SharedMemory in this register."""
        for segment in self.shared_memories.values():
            segment.close()
            segment.unlink()

        self.shared_memories.clear()


# Process Register #
# Register to hold SharedMemories that will be unlinked when this process dies.
PROCESS_SHARED_MEMORY_REGISTER: SharedMemoryRegister = SharedMemoryRegister()
