""" sharedarray.py

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
from contextlib import contextmanager
from multiprocessing.shared_memory import SharedMemory
from typing import Any

# Third-Party Packages #
import numpy as np

# Local Packages #
from ..synchronize import AsyncLock
from .lockedsharedmemory import LockedSharedMemory
from .shareddarray import SharedArray


# Definitions #
# Classes #
class LockedSharedArray(SharedArray):
    """haredArray with a Lock to ensure it is thread and process safe.

    Class Attributes:
        shared_memory_type: The type of SharedMemory this class will use in its instances.

    Attributes:
        _acquired: Determines if the current process has the Lock acquired.

    Args:
        a: An optional array to set the values of this array to.
        shape: The shape to set the array to.
        name: The shared memory name.
        dtype: The data type of the array.
        offset: The offset of array data in the buffer.
        strides: The strides of data in memory
        order: Row-major (C-style) or column-major (Fortran-style) order.
        init: Determines if this object should be initialized.
    """
    shared_memory_type: type[LockedSharedMemory] = LockedSharedMemory

    # Class Methods #
    # Wrapped Attribute Callback Functions
    @classmethod
    def _get_attribute(cls, obj: Any, wrap_name: str, attr_name: str) -> Any:
        """Gets an attribute from a wrapped ndarray.

        Args:
            obj: The target object to get the wrapped object from.
            wrap_name: The attribute name of the wrapped object.
            attr_name: The attribute name of the attribute to get from the wrapped object.

        Returns:
            The wrapped object.
        """
        if obj._acquired:
            return super()._get_attribute(obj, wrap_name, attr_name)
        else:
            with obj:  # Ensures the lock is acquired accessing attributes
                return super()._get_attribute(obj, wrap_name, attr_name)

    @classmethod
    def _set_attribute(cls, obj: Any, wrap_name: str, attr_name: str, value: Any) -> None:
        """Sets an attribute in a wrapped ndarray.

        Args:
            obj: The target object to set.
            wrap_name: The attribute name of the wrapped object.
            attr_name: The attribute name of the attribute to set from the wrapped object.
            value: The object to set the wrapped fileobjects attribute to.
        """
        if obj._acquired:
            super()._set_attribute(obj, wrap_name, attr_name, value)
        else:
            with obj:  # Ensures the lock is acquired accessing attributes
                super()._set_attribute(obj, wrap_name, attr_name, value)

    @classmethod
    def _del_attribute(cls, obj: Any, wrap_name: str, attr_name: str) -> None:
        """Deletes an attribute in a wrapped ndarray.

        Args:
            obj: The target object to set.
            wrap_name: The attribute name of the wrapped object.
            attr_name: The attribute name of the attribute to delete from the wrapped object.
        """
        if obj._acquired:
            super()._del_attribute(obj, wrap_name, attr_name)
        else:
            with obj:  # Ensures the lock is acquired accessing attributes
                super()._del_attribute(obj, wrap_name, attr_name)

    @classmethod
    def _evaluate_method(cls, obj: Any, wrap_name: str, method_name: str, args: Any, kwargs: dict[str, Any]) -> Any:
        """Evaluates a method from a wrapped object.

        Args:
            obj: The target object to get the wrapped object from.
            wrap_name: The attribute name of the wrapped object.
            method_name: The method name of the method to get from the wrapped object.
            args: The args of the method to evaluate.
            kwargs: The keyword arguments of the method to evaluate.

        Returns:
            The wrapped object.
        """
        if obj._acquired:
            return getattr(getattr(obj, wrap_name), method_name)(*args, **kwargs)
        else:
            with obj:  # Ensures the lock is acquired accessing attributes
                return getattr(getattr(obj, wrap_name), method_name)(*args, **kwargs)

    # Magic Methods #
    # Construction/Destruction
    def __init__(
        self,
        a: np.ndarray | None = None,
        shape: Iterable[int, ...] | None = None,
        name: str | None = None,
        dtype: str | np.dtype | None = None,
        offset: int = 0,
        strides: Iterable[int, ...] | None = None,
        order: str | None = None,
        init: bool = True,
    ) -> None:
        # New Attributes #
        self._acquired: bool = False

        # Parent Attributes #
        super().__init__(init=False)

        # Construct #
        if init:
            self.construct(
                a=a,
                shape=shape,
                name=name,
                dtype=dtype,
                offset=offset,
                strides=strides,
                order=order
            )

    @property
    def lock(self) -> AsyncLock | None:
        """The Lock for this SharedArray."""
        return None if self._shared_memory is None else self._shared_memory.lock

    @property
    def references(self) -> int | None:
        """The number of references to this LockedSharedArray"""
        return None if self._shared_memory is None else self._shared_memory.references

    # Context Managers
    def __enter__(self) -> "LockedSharedArray":
        """The context enter which acquires the lock.

        Returns:
            This object.
        """
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """The context exit which releases the lock."""
        self.release()

    # Array
    def __array_function__(self, func, types, args, kwargs) -> Any:
        """Handles how numpy functions are executed with this object.

        Args:
            func: The function to execute.
            types: The types of the args and kwargs.
            args: The arguments to the function.
            kwargs: The keyword arguments to the function.

        Returns:
            The result of the numpy function
        """
        if self._acquired:
            return func(
                *(self._array if arg is self else arg for arg in args),
                **{k: self._array if v is self else v for k, v in kwargs.items()},
            )
        else:
            with self:
                return func(
                    *(self._array if arg is self else arg for arg in args),
                    **{k: self._array if v is self else v for k, v in kwargs.items()},
                )

    # Instance Methods #
    def copy_array(self) -> np.ndarray:
        """Copies the array to a ndarray not it SharedMemory.

        Returns:
            A new copy not in SharedMemory.
        """
        if self._acquired:
            return self._array.copy()
        else:
            with self:
                return self._array.copy()

    # Lock
    def as_unlocked(self) -> SharedArray:
        """Returns a SharedArray with no lock."""
        return SharedArray(shape=self.shape, name=self.name)

    def acquire(self, block: bool = True, timeout: float | None = None) -> bool:
        """Acquires the Lock, waits for the lock if block is True.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.

        Returns:
            If this method successful acquired or failed to a timeout.
        """
        self._acquired = self._shared_memory.acquire(block=block, timeout=timeout)
        return self._acquired

    async def acquire_async(self, block: bool = True, timeout: float | None = None, interval: float = 0.0) -> bool:
        """Asynchronously acquires the Lock, waits for the lock if block is True.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            If this method successful acquired or failed to a timeout.
        """
        self._acquired = await self.lock.acquire_async(block=block, timeout=timeout, interval=interval)
        return self._acquired

    def release(self) -> None:
        """Releases the lock."""
        self._acquired = False
        self.lock.release()
    
    @contextmanager
    async def async_context(
        self,
        block: bool = True,
        timeout: float | None = None,
        interval: float = 0.0,
    ) -> "LockedSharedMemory":
        """Asynchronous context manager for acquiring and releasing the Lock.

        Args:
            block: Determines if this method will block execution while waiting for the lock to be acquired.
            timeout: The time, in seconds, to wait for the lock to be acquired, otherwise returns False.
            interval: The time, in seconds, between each set check.

        Returns:
            This Lock object.
        """
        try:
            await self.acquire_async(block=block, timeout=timeout, interval=interval)
            yield self
        finally:
            self.release()
