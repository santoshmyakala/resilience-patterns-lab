import random

import pytest

from lab import deadline as dl
from lab.retry import RetryableError, RetryBudget, RetryPolicy, backoff_delay


def test_full_jitter_stays_within_exponential_envelope():
    rng = random.Random(7)
    for attempt, upper in [(1, 0.2), (2, 0.4), (3, 0.8), (4, 1.6), (5, 2.0), (9, 2.0)]:
        delays = [backoff_delay(attempt, base=0.2, cap=2.0, rng=rng) for _ in range(2000)]
        assert min(delays) >= 0 and max(delays) <= upper
        # full jitter spreads retries across the whole window (no synchronised thundering herd)
        assert max(delays) > upper * 0.9 and min(delays) < upper * 0.1


def test_retries_until_success_and_counts_attempts():
    calls, slept = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RetryableError("503")
        return "ok"

    assert RetryPolicy(attempts=3, sleep=slept.append, rng=random.Random(1)).call(flaky) == "ok"
    assert len(calls) == 3 and len(slept) == 2


def test_gives_up_after_max_attempts():
    calls = []

    def down():
        calls.append(1)
        raise RetryableError("503")

    with pytest.raises(RetryableError):
        RetryPolicy(attempts=3, sleep=lambda s: None).call(down)
    assert len(calls) == 3


def test_non_retryable_errors_are_not_retried():
    calls = []

    def bad_request():
        calls.append(1)
        raise ValueError("400")

    with pytest.raises(ValueError):
        RetryPolicy(attempts=5, sleep=lambda s: None).call(bad_request)
    assert len(calls) == 1


def test_never_sleeps_past_the_deadline(clock):
    dl.set_clock(clock)
    try:
        calls = []

        def down():
            calls.append(1)
            raise RetryableError("timeout")

        policy = RetryPolicy(attempts=10, base=1.0, cap=1.0, sleep=clock.advance, rng=random.Random(3))
        with dl.deadline(0.05), pytest.raises(RetryableError):
            policy.call(down)
        assert len(calls) < 10  # stopped early: a further backoff would have overrun the budget
    finally:
        import time

        dl.set_clock(time.monotonic)


def test_retry_budget_caps_retries_at_ten_percent_of_requests(clock):
    budget = RetryBudget(ratio=0.1, min_per_second=0.0, max_tokens=1000, clock=clock)
    leaf_calls = 0

    def down():
        nonlocal leaf_calls
        leaf_calls += 1
        raise RetryableError("down")

    policy = RetryPolicy(attempts=3, budget=budget, sleep=lambda s: None)
    for _ in range(1000):
        with pytest.raises(RetryableError):
            policy.call(down)
    # without a budget: 3000 calls. With it: 1000 first attempts + at most ~10% retries.
    assert leaf_calls <= 1000 * 1.1 + 1
