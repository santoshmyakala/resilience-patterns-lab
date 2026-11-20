"""Request deadlines that shrink as a request travels downstream.

Each hop receives the *remaining* budget of its caller instead of a fresh timeout, so a slow
dependency cannot keep work running after the client has already given up.
"""

from __future__ import annotations

import contextvars
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

HEADER = "X-Request-Deadline-Ms"  # remaining budget in milliseconds, set by the caller


class DeadlineExceeded(Exception):
    """The request's time budget is spent; stop work instead of starting new calls."""


_deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar("deadline", default=None)
_clock: Callable[[], float] = time.monotonic


def set_clock(clock: Callable[[], float]) -> None:
    global _clock
    _clock = clock


@contextmanager
def deadline(seconds: float) -> Iterator[None]:
    """Run a block with a budget. Nested blocks can only shorten, never extend, the outer budget."""
    outer = _deadline.get()
    candidate = _clock() + seconds
    token = _deadline.set(candidate if outer is None else min(outer, candidate))
    try:
        yield
    finally:
        _deadline.reset(token)


def remaining() -> float | None:
    """Seconds left, or None when no deadline is active."""
    d = _deadline.get()
    return None if d is None else max(0.0, d - _clock())


def check() -> None:
    if remaining() == 0.0:
        raise DeadlineExceeded("request deadline exceeded")


def timeout_for(default: float) -> float:
    """Timeout for the next downstream call: the smaller of its own limit and the remaining budget."""
    r = remaining()
    if r is not None and r <= 0:
        raise DeadlineExceeded("request deadline exceeded")
    return default if r is None else min(default, r)


def header_value() -> str | None:
    """Value to propagate to the next hop."""
    r = remaining()
    return None if r is None else str(int(r * 1000))
