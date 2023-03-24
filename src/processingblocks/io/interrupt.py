""" interrupt.py

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
from multiprocessing import get_context
from multiprocessing.synchronize import Event
from multiprocessing.context import BaseContext

# Third-Party Packages #
from baseobjects import BaseObject, BaseDict

# Local Packages #


# Definitions #
# Classes #
class Interrupt(Event):
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, parent: Event | None = None, *, ctx: BaseContext | None = None) -> None:
        # New Attributes #
        self.parent: Event | None = parent

        # Construction #
        super().__init__(ctx=get_context() if ctx is None else ctx)

    def __bool__(self) -> bool:
        return self.is_set()

    # Instance Methods #
    def is_set(self) -> bool:
        if self.parent is not None and self.parent.is_set():
            self.set()
        return super().is_set()


class Interrupts(BaseDict):
    # Magic Methods #
    # Construction/Destruction
    def __init__(self, dict_: dict | None = None, /, *args, **kwargs):
        self.master_interrupt = Interrupt()

        super().__init__(dict_, *args, **kwargs)

    # Instance Methods #
    def set(self, name: str, interrupt: Interrupt | None) -> None:
        self.data[name] = interrupt

    def require(self, name: str, interrupt: Interrupt | None = None) -> Interrupt:
        item = self.data.get(name, None)
        if item is None:
            self.data[name] = item = Interrupt(parent=self.master_interrupt) if interrupt is None else interrupt
        return item

    def remove(self, name: str) -> None:
        del self.data[name]

    def is_set(self, name: str) -> bool:
        return self.get(name).is_set()

    def interrupt(self, name: str) -> None:
        self.data[name].set()

    def interrupt_all(self) -> None:
        for interrupt in self.data:
            interrupt.set()

    def interrupt_master(self) -> None:
        self.master_interrupt.set()

    def reset(self, name: str) -> None:
        self.data[name].clear()

    def reset_all(self) -> None:
        for interrupt in self.data:
            interrupt.reset()

    def reset_master(self):
        self.master_interrupt.clear()
