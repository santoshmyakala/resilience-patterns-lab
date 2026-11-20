"""Retry amplification: why only one layer should retry.

Client → Service A → Service B → Service C (down). If every layer makes 3 attempts, one user
request becomes 3 × 3 × 3 = 27 calls to C — exactly when C is least able to take them.
"""

import pytest

from lab.retry import NO_RETRY, RetryableError, RetryBudget, RetryPolicy


def build_chain(policies, leaf):
    call = leaf
    for policy in reversed(policies):
        call = (lambda p, nxt: lambda: p.call(nxt))(policy, call)
    return call


def failing_leaf():
    calls = {"n": 0}

    def leaf():
        calls["n"] += 1
        raise RetryableError("service C unavailable")

    return leaf, calls


def no_sleep(attempts):
    return RetryPolicy(attempts=attempts, sleep=lambda s: None)


def test_every_layer_retrying_multiplies_load_27x():
    leaf, calls = failing_leaf()
    request = build_chain([no_sleep(3), no_sleep(3), no_sleep(3)], leaf)  # client, A, B (B calls C)
    with pytest.raises(RetryableError):
        request()
    assert calls["n"] == 27


def test_retrying_only_at_the_edge_keeps_it_linear():
    leaf, calls = failing_leaf()
    request = build_chain([no_sleep(3), NO_RETRY, NO_RETRY], leaf)
    with pytest.raises(RetryableError):
        request()
    assert calls["n"] == 3


def test_retry_budget_bounds_load_during_an_outage(clock):
    leaf, calls = failing_leaf()
    edge = RetryPolicy(attempts=3, budget=RetryBudget(ratio=0.1, min_per_second=0, clock=clock), sleep=lambda s: None)
    request = build_chain([edge, NO_RETRY, NO_RETRY], leaf)
    for _ in range(500):
        with pytest.raises(RetryableError):
            request()
    # 500 user requests during an outage → ≈550 calls to C, not 1,500 (edge retries) or 13,500 (every layer)
    assert calls["n"] <= 551
