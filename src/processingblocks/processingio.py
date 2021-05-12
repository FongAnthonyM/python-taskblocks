#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processingio.py
Description:
"""
__author__ = "Anthony Fong"
__copyright__ = "Copyright 2021, Anthony Fong"
__credits__ = ["Anthony Fong"]
__license__ = ""
__version__ = "0.1.0"
__maintainer__ = "Anthony Fong"
__email__ = ""
__status__ = "Prototype"

# Default Libraries #
import asyncio
import collections
import multiprocessing
import multiprocessing.connection
from multiprocessing import Process, Pool, Lock, Event, Queue, Pipe
import queue
import warnings
import time

# Downloaded Libraries #

# Local Libraries #


# Todo: Add cross instance socket reader
# Definitions #
# Classes #
class Interrupt(object):
    # Construction/Destruction
    def __init__(self, master=None):
        self.master = master
        self.event = Event()

    def __bool__(self):
        return self.check()

    # Methods
    def check(self):
        if self.master:
            self.event.set()
        return self.event.is_set()

    def set(self):
        self.event.set()

    def reset(self):
        self.event.clear()


class Interrupts(collections.UserDict):
    # Construction/Destruction
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.master_interrupt = Interrupt()

    # Methods
    def add(self, name):
        if name not in self.data:
            self.data[name] = Interrupt(master=self.master_interrupt)
        return self.data[name]

    def set(self, name, interrupt=None):
        if interrupt is None:
            self.data[name] = Interrupt(self.master_interrupt)
        else:
            self.data[name] = interrupt
        return self.data[name]

    def remove(self, name):
        del self.data[name]

    def check(self, name):
        return bool(self.get(name))

    def interrupt(self, name):
        self.data[name].set()

    def interrupt_all(self):
        for interrupt in self.data:
            interrupt.set()

    def interrupt_all_processes(self):
        self.master_interrupt.set()

    def reset(self, name):
        self.data[name].reset()

    def reset_all(self):
        for interrupt in self.data:
            interrupt.reset()

    def reset_all_processes(self):
        self.master_interrupt.reset()


class BroadcastPipe(object):
    # Construction/Destruction
    def __init__(self, name):
        self.name = name

        self.send_connections = {}
        self.recv_connections = {}

    # Pipe
    def create_pipe(self, name, duplex=True):
        self.send_connections[name], self.recv_connections[name] = Pipe(duplex=duplex)
        return self.send_connections[name], self.recv_connections[name]

    def set_connections(self, name, send, recv):
        self.send_connections[name] = send
        self.recv_connections[name] = recv

    def set_send_connection(self, name, send):
        self.send_connections[name] = send

    def set_recv_connection(self, name, recv):
        self.recv_connections[name] = recv

    def get_send_connection(self, name):
        return self.send_connections[name]

    def get_recv_connection(self, name):
        return self.recv_connections[name]

    # Object Query
    def poll(self):
        output = {}
        for name, connection in self.recv_connections.items():
            output[name] = connection.poll()
        return output

    def all_empty(self):
        for connection in self.recv_connections.values():
            if not connection.poll():
                return False
        return True

    def any_empty(self):
        for connection in self.recv_connections.values():
            if connection.poll():
                return True
        return False

    # Transmission
    def send(self, obj):
        for connection in self.send_connections.values():
            connection.send(obj)

    def send_bytes(self, obj, **kwargs):
        for connection in self.send_connections.values():
            connection.send_bytes(obj, **kwargs)

    def recv(self, name, poll=True, timeout=0.0):
        connection = self.recv_connections[name]
        if not poll or connection.poll(timeout=timeout):
            return connection.recv()
        else:
            return None

    def recv_wait(self, name, timeout=None, interval=0.0):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            time.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv()

    async def recv_wait_async(self, name, timeout=None, interval=0.0):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv()

    def recv_bytes(self, name, poll=True, timeout=0.0, **kwargs):
        connection = self.recv_connections[name]
        if not poll or connection.poll(timeout=timeout):
            return connection.recv(**kwargs)
        else:
            return None

    def recv_bytes_wait(self, name, timeout=None, interval=0.0, **kwargs):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            time.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv_bytes(**kwargs)

    async def recv_bytes_wait_async(self, name, timeout=None, interval=0.0, **kwargs):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv_bytes(**kwargs)

    def clear_recv(self, name):
        connection = self.recv_connections[name]
        while connection.poll:
            connection.recv()


class BroadcastQueue(object):
    # Construction/Destruction
    def __init__(self, name):
        self.name = name

        self.queues = {}

    # Queue
    def create_queue(self, name, maxsize=None):
        self.queues[name] = Queue(maxsize=maxsize)
        return self.queues[name]

    def set_queue(self, name, q):
        self.queues[name] = q

    def get_queue(self, name):
        return self.queues[name]

    # Object Query
    def qsize(self):
        output = {}
        for name, q in self.queues.items():
            output[name] = q.qsize()
        return output

    def empty(self):
        output = {}
        for name, q in self.queues.items():
            output[name] = q.empty()
        return output

    def all_empty(self):
        for q in self.queues.values():
            if not q.empty():
                return False
        return True

    def any_empty(self):
        for q in self.queues.values():
            if q.empty():
                return True
        return True

    def all_full(self):
        for q in self.queues.values():
            if not q.full():
                return False
        return True

    def any_full(self):
        for q in self.queues.values():
            if q.full():
                return True
        return False

    # Transmission
    def put(self, obj, block=False, timeout=0.0):
        for q in self.queues.values():
            try:
                q.put(obj, block=block, timeout=timeout)
            except queue.Full:
                pass  # add a warning here

    def get(self, name, block=True, timeout=0.0):
        return self.queues[name].get(block=block, timeout=timeout)

    def get_wait(self, name, timeout=None, interval=0.0):
        connection = self.queues[name]
        start_time = time.perf_counter()
        while connection.empty():
            time.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.get()

    async def get_wait_async(self, name, timeout=None, interval=0.0):
        connection = self.queues[name]
        start_time = time.perf_counter()
        while connection.empty():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.get()


class InputsHandler(object):
    # Construction/Destruction
    def __init__(self, name=""):
        self.name = name

        self.interrupts = Interrupts()

        self.inputs = {}
        self.events = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.inputs[item]

    # Constructors/Destructors
    def destruct(self):
        self.stop_all()

    # Events
    def create_event(self, name):
        self.inputs[name] = Event()
        self.events[name] = self.inputs[name]
        return self.inputs[name]

    def add_event(self, name, event):
        self.inputs[name] = event
        self.events[name] = event

    def clear_events(self):
        for event in self.events:
            del self.inputs[event]
        self.events.clear()

    def wait_for_event(self, name, reset=True, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if event.is_set():
                if reset:
                    event.clear()
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_event_async(self, name, reset=True, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if event.is_set():
                if reset:
                    event.clear()
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Queues
    def create_queue(self, name, maxsize=0):
        self.inputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.inputs[name]
        return self.inputs[name]

    def add_queue(self, name, q):
        self.inputs[name] = q
        self.queues[name] = q

    def clear_queues(self):
        for q in self.queues:
            del self.inputs[q]
        self.queues.clear()

    def wait_for_queue(self, name, timeout=None, interval=0.0):
        q = self.queues[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            try:
                return q.get(block=False)
            except queue.Empty:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_queue_async(self, name, timeout=None, interval=0.0):
        q = self.queues[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            try:
                return q.get(block=False)
            except queue.Empty:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Pipes
    def create_pipe(self, name, duplex=True):
        output, self.inputs[name] = Pipe(duplex=duplex)
        self.pipes[name] = self.inputs[name]
        return output

    def add_pipe(self, name, pipe):
        self.inputs[name] = pipe
        self.pipes[name] = pipe

    def clear_pipes(self):
        for pipe in self.pipes:
            del self.inputs[pipe]
        self.pipes.clear()

    def wait_for_pipe(self, name, timeout=None, interval=0.0):
        connection = self.pipes[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_pipe_async(self, name, timeout=None, interval=0.0):
        connection = self.pipes[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Broadcasters
    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        _, self.inputs[name] = broadcaster.create_pipe(name)
        self.broadcasters[name] = self.inputs[name]
        return broadcaster

    def add_broadcast(self, name, broadcaster):
        if isinstance(broadcaster, BroadcastPipe):
            if name not in broadcaster.recv_connections:
                broadcaster.create_pipe(name)
            self.inputs[name] = broadcaster.recv_connections[name]
        else:
            self.inputs[name] = broadcaster
        self.broadcasters[name] = self.inputs[name]

    def clear_broadcasts(self):
        for broadcast in self.broadcasters:
            del self.inputs[broadcast]
        self.broadcasters.clear()

    def wait_for_broadcast(self, name, timeout=None, interval=0.0):
        connection = self.broadcasters[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_broadcast_async(self, name, timeout=None, interval=0.0):
        connection = self.broadcasters[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # All
    def clear_all(self):
        self.inputs.clear()
        self.events.clear()
        self.queues.clear()
        self.pipes.clear()
        self.broadcasters.clear()

    # Transmission
    def get_item(self, name, reset=True, **kwargs):
        if name in self.events:
            if self.events[name].is_set():
                if reset:
                    self.events[name].clear()
                return True
            else:
                return False
        elif name in self.queues:
            return self.safe_queue_get(self.queues[name], **kwargs)
        elif name in self.pipes:
            return self.safe_pipe_recv(self.pipes[name], **kwargs)
        elif name in self.broadcasters:
            return self.safe_pipe_recv(self.broadcasters[name], **kwargs)
        else:
            warnings.warn()

    def get_item_wait(self, name, timeout=None, interval=0.0, reset=True):
        if name in self.events:
            return self.wait_for_event(name=name, reset=reset, timeout=timeout, interval=interval)
        elif name in self.queues:
            return self.wait_for_queue(name=name, timeout=timeout, interval=interval)
        elif name in self.pipes:
            return self.wait_for_pipe(name=name, timeout=timeout, interval=interval)
        elif name in self.broadcasters:
            return self.wait_for_broadcast(name=name, timeout=timeout, interval=interval)
        else:
            warnings.warn()

    async def get_item_wait_async(self, name, timeout=None, interval=0.0, reset=True):
        if name in self.events:
            return await self.wait_for_event_async(name=name, reset=reset, timeout=None, interval=interval)
        if name in self.queues:
            return await self.wait_for_queue_async(name=name, timeout=timeout, interval=interval)
        elif name in self.pipes:
            return await self.wait_for_pipe_async(name=name, timeout=timeout, interval=interval)
        elif name in self.broadcasters:
            return await self.wait_for_broadcast_async(name=name, timeout=timeout, interval=interval)
        else:
            warnings.warn()

    def stop_all(self):
        self.interrupts.interrupt_all_processes()

    @staticmethod
    def safe_pipe_recv(pipe, poll=True, timeout=0.0):
        if not poll or pipe.poll(timeout=timeout):
            return pipe.recv()
        else:
            return None

    @staticmethod
    def safe_queue_get(q, block=False, timeout=None):
        try:
            return q.get(block=block, timeout=timeout)
        except queue.Empty:
            return None


class OutputsHandler(object):
    # Construction/Destruction
    def __init__(self, name=""):
        self.name = name

        self.interrupts = Interrupts()

        self.outputs = {}
        self.events = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.outputs[item]

    # Constructors/Destructors
    def destruct(self):
        self.stop_all()

    # Events
    def create_event(self, name):
        self.outputs[name] = Event()
        self.events[name] = self.outputs[name]
        return self.outputs[name]

    def add_event(self, name, event):
        self.outputs[name] = event
        self.events[name] = event

    def clear_events(self):
        for event in self.events:
            del self.outputs[event]
        self.events.clear()

    def event_wait(self, name, timeout=None, interval=0.0):
        self.events[name].set()
        return self.wait_for_event_clear(name=name, timeout=timeout, interval=interval)

    async def event_wait_async(self, name, timeout=None, interval=0.0):
        self.events[name].set()
        return await self.wait_for_event_clear_async(name=name, timeout=timeout, interval=interval)

    def wait_for_event_clear(self, name, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if not event.is_set():
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_event_clear_async(self, name, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt.is_set():
            if not event.is_set():
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Queues
    def create_queue(self, name, maxsize=0):
        self.outputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.outputs[name]
        return self.outputs[name]

    def add_queue(self, name, q):
        self.outputs[name] = q
        self.queues[name] = q

    def clear_queues(self):
        for q in self.queues:
            del self.outputs[q]
        self.queues.clear()

    # Pipes
    def create_pipe(self, name, duplex=True):
        self.outputs[name], input_ = Pipe(duplex=duplex)
        self.pipes[name] = self.outputs[name]
        return input_

    def add_pipe(self, name, pipe):
        self.outputs[name] = pipe
        self.pipes[name] = pipe

    def clear_pipes(self):
        for pipe in self.pipes:
            del self.outputs[pipe]
        self.pipes.clear()

    # Broadcasters
    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        self.outputs[name] = broadcaster
        self.broadcasters[name] = broadcaster
        return broadcaster

    def add_broadcast(self, name, broadcaster):
        self.outputs[name] = broadcaster
        self.broadcasters[name] = broadcaster

    def clear_broadcasts(self):
        for broadcast in self.broadcasters:
            del self.outputs[broadcast]
        self.broadcasters.clear()

    # All
    def clear_all(self):
        self.outputs.clear()
        self.events.clear()
        self.queues.clear()
        self.pipes.clear()
        self.broadcasters.clear()

    # Transmission
    def send_item(self, name, item, **kwargs):
        if name in self.events:
            self.events[name].set()
        elif name in self.queues:
            return self.queues[name].put(item, **kwargs)
        elif name in self.pipes:
            return self.pipes[name].send(item, **kwargs)
        elif name in self.broadcasters:
            return self.broadcasters[name].send(item, **kwargs)
        else:
            warnings.warn()

    def stop_all(self):
        self.interrupts.interrupt_all_processes()


class InstanceIOServer(object):
    # Construction/Destruction
    def __init__(self, address=None, family=None, backlog=1, authkey=None, init=True):
        self.listener_kwargs = {"address": address, "family": family, "backlog": backlog, "authkey": authkey}

        self.listener = None

        if init:
            self.construct()

    # Constructors/Destructors
    def construct(self, **kwargs):
        for key in self.listener_kwargs:
            if key in kwargs:
                self.listener_kwargs[key] = kwargs[key]

        self.listener = multiprocessing.connection.Listener(**self.listener_kwargs)

    def close(self):
        self.listener.close()


class InstanceIOClient(object):
    # Construction/Destruction
    def __init__(self):
        pass

    # Constructors/Destructors


# Main #
if __name__ == "__main__":
    pass
