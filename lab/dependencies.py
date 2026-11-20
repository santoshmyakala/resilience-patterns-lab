"""Clients for the two external dependencies, each wrapped in the resilience patterns.

* Fraud check (synchronous, on the request path): deadline-bounded timeout, budgeted retries,
  circuit breaker, and a *degraded fallback* (send the payment to manual review) instead of failing.
* Payment provider (called by the worker): every capture carries an idempotency key, so a
  redelivered message can never charge twice.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

import httpx

from . import deadline as dl
from .breaker import CircuitBreaker, CircuitOpen, State
from .metrics import BREAKER_STATE, DEGRADED, RETRIES, RETRY_BUDGET_EXHAUSTED
from .retry import RetryableError, RetryBudget, RetryPolicy


class FraudClient:
    def __init__(
        self,
        base_url: str,
        http: httpx.Client | None = None,
        *,
        breaker: CircuitBreaker | None = None,
        retry: RetryPolicy | None = None,
        per_call_timeout: float = 0.3,
    ):
        self.http = http or httpx.Client(base_url=base_url)
        self.per_call_timeout = per_call_timeout
        self.breaker = breaker or CircuitBreaker(
            "fraud",
            failure_threshold=5,
            reset_after=30,
            on_state_change=lambda n, s: BREAKER_STATE.labels(n).set(0 if s is State.CLOSED else 1),
        )
        self.retry = retry or RetryPolicy(
            attempts=2,
            base=0.05,
            cap=0.2,
            budget=RetryBudget(ratio=0.1),
            on_retry=lambda *_: RETRIES.labels("fraud").inc(),
            on_budget_exhausted=lambda: RETRY_BUDGET_EXHAUSTED.labels("fraud").inc(),
        )

    def _attempt(self, payload: dict) -> str:
        headers = {}
        if (h := dl.header_value()) is not None:
            headers[dl.HEADER] = h
        try:
            r = self.http.post("/check", json=payload, headers=headers, timeout=dl.timeout_for(self.per_call_timeout))
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise RetryableError(str(exc)) from exc
        if r.status_code in (429, 502, 503, 504):
            raise RetryableError(f"fraud service {r.status_code}")
        r.raise_for_status()
        return r.json()["decision"]

    def decide(self, customer_id: str, amount_cents: int) -> str:
        """Return 'approve' or 'review'. Never raises for dependency failures: degrades instead."""
        payload = {"customer_id": customer_id, "amount_cents": amount_cents}
        try:
            return self.retry.call(
                lambda: self.breaker.call(lambda: self._attempt(payload), failure_on=(RetryableError,))
            )
        except CircuitOpen:
            DEGRADED.labels("fraud_circuit_open").inc()
        except RetryableError:
            DEGRADED.labels("fraud_unavailable").inc()
        except dl.DeadlineExceeded:
            DEGRADED.labels("deadline").inc()
        return "review"  # graceful degradation: accept, but hold for manual review


@dataclass
class FakePaymentProvider:
    """Stands in for a PSP that supports idempotency keys (as Stripe, Adyen etc. do)."""

    charges: dict[str, int] = field(default_factory=dict)
    calls: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def capture(self, idempotency_key: str, amount_cents: int) -> str:
        with self._lock:
            self.calls += 1
            self.charges.setdefault(idempotency_key, amount_cents)  # same key → same charge, never a second one
            return f"ch_{idempotency_key}"
