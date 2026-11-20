"""Admission control (load shedding): cap concurrent work and reject the excess immediately.

Queueing beyond capacity only converts overload into latency for everyone. Rejecting early with
503 + Retry-After keeps accepted requests inside their latency objective and tells clients to back off.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager


class Overloaded(Exception):
    pass


class ConcurrencyLimiter:
    def __init__(self, max_in_flight: int):
        self.max_in_flight = max_in_flight
        self._sem = threading.BoundedSemaphore(max_in_flight)
        self._lock = threading.Lock()
        self.in_flight = 0

    @contextmanager
    def slot(self) -> Iterator[None]:
        if not self._sem.acquire(blocking=False):
            raise Overloaded(f"more than {self.max_in_flight} requests in flight")
        with self._lock:
            self.in_flight += 1
        try:
            yield
        finally:
            with self._lock:
                self.in_flight -= 1
            self._sem.release()
