""" sharedmemory.py
Extends SharedMemory with a reference count and optional registration for unlinking when the process dies.
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
from multiprocessing.shared_memory import SharedMemory as SharedMemory_
from multiprocessing import Value

# Third-Party Packages #

# Local Packages #
from .sharedmemoryregister import PROCESS_SHARED_MEMORY_REGISTER


# Definitions #
# Classes #
class SharedMemory(SharedMemory_):
    """Extends SharedMemory with a reference count and optional registration for unlinking when the process dies.

    Attributes:
        _references: A optional field to track the number of references to this object.

    Args:
        name: The name of the SharedMemory.
        create: Determines if the SharedMemory will be created if it does not exist.
        size: The number of bytes for this SharedMemory to allocate.
        register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.
    """
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, name: str | None = None, create: bool = False, size: int = 0, register: bool = True) -> None:
        # New Attributes #
        self._references: Value = Value("q")

        # Parent Attributes #
        super().__init__(name=name, create=create, size=size)
        if register:
            PROCESS_SHARED_MEMORY_REGISTER.register_shared_memory(self)

    @property
    def references(self) -> int:
        """The number of references to this SharedMemory"""
        with self._references:
            return self._references.value

    # Instance Methods #
    # References
    def increment_reference(self, i: int = 1) -> None:
        """Increases the references.

        Args:
            i: The amount to increment by.
        """
        with self._references:
            self._references.value += i

    def decrement_reference(self, i: int = 1) -> None:
        """Decreases the references.

        Args:
            i: The amount to decrement by.
        """
        with self._references:
            self._references.value -= i
