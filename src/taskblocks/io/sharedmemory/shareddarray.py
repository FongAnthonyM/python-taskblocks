""" sharedarray.py
A wrapper for a numpy ndarray which allocates it in SharedMemory.
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
from collections.abc import Iterable, Mapping
from typing import Any

# Third-Party Packages #
from baseobjects.wrappers import StaticWrapper
import numpy as np

# Local Packages #
from .sharedmemory import SharedMemory


# Definitions #
# Classes #
class SharedArray(StaticWrapper):
    """A wrapper for a numpy ndarray which allocates it in SharedMemory.

    Class Attributes:
        shared_memory_type: The type of SharedMemory this class will use in its instances.

    Attributes:
        _offset: The offset of array data in the buffer.
        _array: The ndarry which this object is wrapping.
        _shared_memory: The SharedMemory which the array is allocated to.

    Args:
        a: An optional array to set the values of this array to.
        shape: The shape to set the array to.
        name: The shared memory name.
        dtype: The data type of the array.
        offset: The offset of array data in the buffer.
        strides: The strides of data in memory
        order: Row-major (C-style) or column-major (Fortran-style) order.
        register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.
        init: Determines if this object should be initialized.
    """
    _wrapped_types: list[type | object] = [np.ndarray]
    _wrap_attributes: list[str] = ["array"]
    _exclude_attributes: set[str] = StaticWrapper._exclude_attributes | {"__array_ufunc__"}
    shared_memory_type: type[SharedMemory] = SharedMemory

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
        register: bool = True,
        init: bool = True,
    ) -> None:
        # New Attributes #
        self._offset: int = 0
        self._array: np.ndarray | None = None
        self._shared_memory: SharedMemory | None = None

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
                order=order,
                register=register,
            )

    @property
    def name(self) -> str:
        """The SharedMemory name."""
        return self._shared_memory.name

    # Pickling
    def __getstate__(self) -> dict[str, Any]:
        """Creates a dictionary of attributes which can be used to rebuild this object.

        Returns:
            dict: A dictionary of this object's attributes.
        """
        state = self.__dict__.copy()
        state["_array"] = None
        state["_shared_memory"] = None
        state["kwargs"] = {
            "name": self._shared_memory.name,
            "shape": self._array.shape,
            "dtype": self._array.dtype,
            "offset": self._offset,
            "strides": self._array.strides,
            "order": "C" if self._array.flags.c_contiguous else "F",
        }

        return state

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        """Builds this object based on a dictionary of corresponding attributes.

        Args:
            state: The attributes to build this object from.
        """
        kwargs = state.pop("kwargs")
        self.__dict__.update(state)

        self.construct_exsisting_array(**kwargs)

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
        return func(
            *(self._array if arg is self else arg for arg in args),
            **{k: self._array if v is self else v for k, v in kwargs.items()},
        )

    # Instance Methods #
    # Constructors/Destructors
    def construct(
        self,
        a: np.ndarray | None = None,
        shape: Iterable[int, ...] | None = None,
        name: str | None = None,
        dtype: str | np.dtype | None = None,
        offset: int | None = None,
        strides: Iterable[int, ...] | None = None,
        order: str | None = None,
        register: bool = True,
    ) -> None:
        """Constructs this object.

        Args:
            a: An optional array to set the values of this array to.
            shape: The shape to set the array to.
            name: The shared memory name.
            dtype: The data type of the array.
            offset: The offset of array data in the buffer.
            strides: The strides of data in memory
            order: Row-major (C-style) or column-major (Fortran-style) order.
            register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.
        """
        if a is not None:
            self.construct_from_array(a=a, name=name, register=register)
        elif shape is not None:
            self.construct_new_array(shape=shape, name=name, strides=strides, order=order, register=register)
        elif name is not None:
            raise ValueError("Either an array or the shape must be provided.")

    def construct_from_array(self, a: np.ndarray, name: str | None = None, register: bool = True) -> None:
        """Constructs this object from a given array. Replaces the values if the SharedMemory already exists.

        Args:
            a: The array to set the values this array to.
            name: The name of this SharedMemory.
            register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.
        """
        try:
            self._shared_memory = self.shared_memory_type(name=name, register=register)
        except (FileNotFoundError, ValueError):
            self._shared_memory = self.shared_memory_type(name=name, create=True, size=int(a.nbytes), register=register)
        self._array = np.ndarray(a.shape, dtype=a.dtype, buffer=self._shared_memory.buf)
        self[:] = a[:]

    def construct_new_array(
        self,
        shape: Iterable[int, ...],
        name: str | None = None,
        dtype: str | np.dtype | None = None,
        offset: int = 0,
        strides: Iterable[int, ...] | None = None,
        order: str | None = None,
        register: bool = True,
    ) -> None:
        """Constructs this object from given parameters.

        Args:
            shape: The shape to set the array to.
            name: The shared memory name.
            dtype: The data type of the array.
            offset: The offset of array data in the buffer.
            strides: The strides of data in memory
            order: Row-major (C-style) or column-major (Fortran-style) order.
            register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.
        """
        try:
            self._shared_memory = self.shared_memory_type(name=name, register=register)
        except (FileNotFoundError, ValueError):
            self._shared_memory = self.shared_memory_type(
                name=name,
                create=True,
                size=int(np.dtype(dtype).itemsize * np.prod(shape)),
                register=register
            )

        self._array = np.ndarray(
            shape,
            dtype=dtype,
            buffer=self._shared_memory.buf,
            offset=offset,
            strides=strides,
            order=order,
        )

    def construct_exsisting_array(
        self,
        shape: Iterable[int, ...],
        name: str | None = None,
        dtype: str | np.dtype | None = None,
        offset: int = 0,
        strides: Iterable[int, ...] | None = None,
        order: str | None = None,
        register: bool = True,
    ) -> None:
        """Constructs this object from given parameters.

        Args:
            shape: The shape to set the array to.
            name: The shared memory name.
            dtype: The data type of the array.
            offset: The offset of array data in the buffer.
            strides: The strides of data in memory
            order: Row-major (C-style) or column-major (Fortran-style) order.
            register: Determines if the SharedMemory will be registered to help deallocate it when the process dies.
        """
        self._shared_memory = self.shared_memory_type(name=name, register=register)

        self._array = np.ndarray(
            shape,
            dtype=dtype,
            buffer=self._shared_memory.buf,
            offset=offset,
            strides=strides,
            order=order,
        )

    def copy_array(self) -> np.ndarray:
        """Copies the array to a ndarray not it SharedMemory.

        Returns:
            A new copy not in SharedMemory.
        """
        return self._array.copy()

    # Shared Memory
    def increment_reference(self, i: int = 1):
        """Increases the references.

        Args:
            i: The amount to increment by.
        """
        self._shared_memory.increment_reference(i=i)

    def decrement_reference(self, i: int = -1):
        """Decreases the references.

        Args:
            i: The amount to decrement by.
        """
        self._shared_memory.decrement_reference(i=i)

    def close(self) -> None:
        """Closes access to the shared memory from this instance but does not destroy the shared memory block."""
        self._shared_memory.close()

    def unlink(self) -> None:
        """Requests that the underlying shared memory block be destroyed.

        In order to ensure proper cleanup of resources, unlink should be called once (and only once) across all
        processes which have access to the shared memory block.
        """
        self._shared_memory.unlink()
