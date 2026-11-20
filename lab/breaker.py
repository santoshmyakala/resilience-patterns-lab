"""Circuit breaker: stop calling a dependency that keeps failing, probe it, recover automatically.

CLOSED    calls pass; ``failure_threshold`` consecutive failures open the circuit.
OPEN      calls fail immediately with CircuitOpen (no timeout spent, no load added) for ``reset_after`` s.
HALF_OPEN one trial call is let through; success closes the circuit, failure re-opens it.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import StrEnum
from typing import TypeVar

T = TypeVar("T")


class State(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpen(Exception):
    """The dependency is considered down; the call was not attempted."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_after: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[str, State], None] | None = None,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_after = reset_after
        self._clock = clock
        self._on_state_change = on_state_change
        self._state = State.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._trial_in_flight = False
        self._lock = threading.Lock()

    @property
    def state(self) -> State:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _set(self, state: State) -> None:
        if state is not self._state:
            self._state = state
            if self._on_state_change:
                self._on_state_change(self.name, state)

    def _maybe_half_open(self) -> None:
        if self._state is State.OPEN and self._clock() - self._opened_at >= self.reset_after:
            self._set(State.HALF_OPEN)
            self._trial_in_flight = False

    def call(self, fn: Callable[[], T], failure_on: tuple[type[BaseException], ...] = (Exception,)) -> T:
        with self._lock:
            self._maybe_half_open()
            if self._state is State.OPEN:
                raise CircuitOpen(self.name)
            if self._state is State.HALF_OPEN:
                if self._trial_in_flight:
                    raise CircuitOpen(self.name)  # only one probe at a time
                self._trial_in_flight = True
        try:
            result = fn()
        except failure_on:
            with self._lock:
                self._failures += 1
                if self._state is State.HALF_OPEN or self._failures >= self.failure_threshold:
                    self._opened_at = self._clock()
                    self._set(State.OPEN)
                self._trial_in_flight = False
            raise
        with self._lock:
            self._failures = 0
            self._trial_in_flight = False
            self._set(State.CLOSED)
        return result
