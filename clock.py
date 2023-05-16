from __future__ import annotations

import abc
from dataclasses import dataclass
import uuid
import collections

from typing import Dict, TypeVar, Generic

PID = uuid.UUID


TimeStamp = TypeVar("TimeStamp")
ExportBundle = TypeVar("ExportBundle")


class AbstractClock(abc.ABC, Generic[TimeStamp, ExportBundle]):
    """Representing an abstract clock with the folowing properties

    # time is monoton
    c = AbstractClock()
    t = c.get_time()
    assert t < c.get_time()

    # time merge is monoton too
    c1 = AbstractClock()
    c2 = AbstractClock()
    ...
    t1 = c1.get_time()
    t2 = c2.get_time()
    c2.set_time(t1)
    t3 = c2.get_time()
    assert t1 < t3
    assert t2 < t3
    """

    @abc.abstractmethod
    def get_time(self) -> TimeStamp:
        """Get the current time"""
        ...

    @abc.abstractmethod
    def seen_time(self, ts: TimeStamp):
        """Set the time

        This operation may be more complicated,
        because it will probably not set the local time.
        Instead it will merge the new timestamp into
        our own representation of time.
        """
        ...

    @abc.abstractmethod
    def export(self) -> ExportBundle:
        ...

    @abc.abstractclassmethod
    def create_from_export(cls, export: ExportBundle):
        ...


# Example Implementation: Lamport Time


@dataclass(order=True, frozen=True)
class _LamportTimeStamp:
    # Note: Order is important here
    # to make comparison work as expected.
    counter: int
    process_id: PID


@dataclass
class _LamportExport:
    pid: PID
    ts: _LamportTimeStamp


class LamportClock(AbstractClock):
    def __init__(self, process_id: (PID | None) = None):
        if process_id is None:
            process_id = uuid.uuid4()
        self.process_id = process_id
        self.counter = 0

    def get_time(self) -> _LamportTimeStamp:
        self.counter += 1
        return _LamportTimeStamp(counter=self.counter, process_id=self.process_id)

    def seen_time(self, ts: _LamportTimeStamp) -> None:
        self.counter = max(self.counter, ts.counter)

    def export(self) -> _LamportExport:
        return _LamportExport(pid=self.process_id, ts=self.get_time())

    @classmethod
    def create_from_export(cls, export: _LamportExport):
        c = cls(process_id=export.pid)
        c.seen_time(export.ts)
        return c


# Example Implementation: Vector Clock


@dataclass(frozen=True, eq=True)
class _VectorTimeStamp:
    timestamps: Dict[PID, int]

    def __lt__(self, other: _VectorTimeStamp) -> bool:
        if set(self.timestamps).issubset(set(other.timestamps)):
            for pid, ts in self.timestamps.items():
                if ts > other.timestamps[pid]:
                    return False
            return self.timestamps != other.timestamps
        return False


@dataclass
class _VectorExport:
    pid: PID
    ts: _VectorTimeStamp


class VectorClock(AbstractClock):
    def __init__(self, process_id: (PID | None) = None):
        if process_id is None:
            process_id = uuid.uuid4()
        self.process_id = process_id
        self.clocks = collections.defaultdict(int)

    def get_time(self) -> _VectorTimeStamp:
        self.clocks[self.process_id] += 1
        return _VectorTimeStamp(timestamps=dict(self.clocks))

    def seen_time(self, ts: _VectorTimeStamp) -> None:
        for pid, ts in ts.timestamps.items():
            self.clocks[pid] = max(self.clocks[pid], ts)

    def export(self) -> _VectorExport:
        return _VectorExport(pid=self.process_id, ts=self.get_time())

    @classmethod
    def create_from_export(cls, export: _VectorExport):
        c = cls(process_id=export.pid)
        c.seen_time(export.ts)
        return c
