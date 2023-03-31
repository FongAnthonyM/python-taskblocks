""" iomanager.py

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

# Third-Party Packages #

# Local Packages #
from .interrupt import Interrupts


# Definitions #
# Classes #
class IOManager(ChainMap):
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, *maps):
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
    # All
    def clear_all(self):
        self.events.clear()
        self.queues.clear()
        self.pipes.clear()
        self.managers.clear()
