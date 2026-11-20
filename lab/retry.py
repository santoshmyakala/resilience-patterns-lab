"""Retries that cannot amplify an outage.

- Exponential backoff with *full jitter* (AWS Architecture Blog, "Exponential Backoff and Jitter"):
  sleep = random(0, min(cap, base * 2**attempt)). Spreads retries out so clients don't synchronise.
- A *retry budget*: retries are allowed only while they stay under a fixed share of recent
  requests. When a dependency is down, almost every call fails, so retries stop instead of
  multiplying the load (the 3 x 3 x 3 = 27 problem).
- Deadline-aware: never sleep or retry past the request deadline.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from . import deadline as dl

T = TypeVar("T")


class RetryableError(Exception):
    """A failure that may succeed if tried again (timeouts, 503, throttling)."""


def backoff_delay(attempt: int, base: float = 0.2, cap: float = 2.0, rng: random.Random | None = None) -> float:
    """Full-jitter delay before retry number ``attempt`` (1 = first retry)."""
    upper = min(cap, base * (2 ** (attempt - 1)))
    return (rng or random).uniform(0, upper)


@dataclass
class RetryBudget:
    """Token bucket: each request deposits ``ratio`` tokens, each retry withdraws one.

    ``min_per_second`` keeps a trickle of retries available at low traffic.
    """

    ratio: float = 0.1
    min_per_second: float = 1.0
    max_tokens: float = 10.0
    clock: Callable[[], float] = time.monotonic
    _tokens: float = field(default=0.0, init=False)
    _last: float | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def _refill(self) -> None:
        now = self.clock()
        if self._last is not None:
            self._tokens = min(self.max_tokens, self._tokens + (now - self._last) * self.min_per_second)
        self._last = now

    def record_request(self) -> None:
        with self._lock:
            self._refill()
            self._tokens = min(self.max_tokens, self._tokens + self.ratio)

    def try_spend(self) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


@dataclass
class RetryPolicy:
    attempts: int = 3  # total attempts, including the first
    base: float = 0.2
    cap: float = 2.0
    budget: RetryBudget | None = None
    sleep: Callable[[float], None] = time.sleep
    rng: random.Random | None = None
    on_retry: Callable[[int, BaseException], None] | None = None
    on_budget_exhausted: Callable[[], None] | None = None

    def call(self, fn: Callable[[], T], retry_on: tuple[type[BaseException], ...] = (RetryableError,)) -> T:
        if self.budget:
            self.budget.record_request()
        attempt = 1
        while True:
            dl.check()
            try:
                return fn()
            except retry_on as exc:
                if attempt >= self.attempts:
                    raise
                if self.budget and not self.budget.try_spend():
                    if self.on_budget_exhausted:
                        self.on_budget_exhausted()
                    raise
                delay = backoff_delay(attempt, self.base, self.cap, self.rng)
                left = dl.remaining()
                if left is not None and delay >= left:
                    raise  # waiting would blow the deadline: fail now, let the caller decide
                if self.on_retry:
                    self.on_retry(attempt, exc)
                self.sleep(delay)
                attempt += 1


NO_RETRY = RetryPolicy(attempts=1)
