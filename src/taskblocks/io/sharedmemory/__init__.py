""" __init__.py
Objects for managing shared memory.
"""
# Package Header #
from ...header import *

# Header #
__author__ = __author__
__credits__ = __credits__
__maintainer__ = __maintainer__
__email__ = __email__


# Imports #
# Local Packages #
from .sharedmemoryregister import SharedMemoryRegister, PROCESS_SHARED_MEMORY_REGISTER
from .sharedmemory import SharedMemory
from .lockedsharedmemory import LockedSharedMemory

try:
    import numpy
except ModuleNotFoundError:
    pass
else:
    from .shareddarray import SharedArray
    from .lockedsharedarray import LockedSharedArray
