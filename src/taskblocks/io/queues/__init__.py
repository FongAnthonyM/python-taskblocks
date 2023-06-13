""" __init__.py
Queue objects.
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
from .asyncqueueinterface import AsyncQueueInterface
from .simpleasyncqueue import SimpleAsyncQueue
from .asyncqueue import AsyncQueue
from .asyncqueuemanager import AsyncQueueManager

try:
    import numpy
except ModuleNotFoundError:
    pass
else:
    from .arrayqueue import ArrayQueue
