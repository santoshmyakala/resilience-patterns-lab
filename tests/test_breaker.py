import pytest

from lab.breaker import CircuitBreaker, CircuitOpen, State


class Boom(Exception):
    pass


def failing():
    raise Boom


def test_opens_after_threshold_and_then_fails_fast(clock):
    cb = CircuitBreaker("dep", failure_threshold=3, reset_after=30, clock=clock)
    for _ in range(3):
        with pytest.raises(Boom):
            cb.call(failing)
    assert cb.state is State.OPEN
    called = []
    with pytest.raises(CircuitOpen):
        cb.call(lambda: called.append(1))
    assert called == []  # no timeout spent, no load added to the struggling dependency


def test_half_open_allows_one_probe_and_closes_on_success(clock):
    cb = CircuitBreaker("dep", failure_threshold=1, reset_after=30, clock=clock)
    with pytest.raises(Boom):
        cb.call(failing)
    clock.advance(30)
    assert cb.state is State.HALF_OPEN
    assert cb.call(lambda: "ok") == "ok"
    assert cb.state is State.CLOSED


def test_failed_probe_reopens(clock):
    cb = CircuitBreaker("dep", failure_threshold=1, reset_after=30, clock=clock)
    with pytest.raises(Boom):
        cb.call(failing)
    clock.advance(30)
    with pytest.raises(Boom):
        cb.call(failing)
    assert cb.state is State.OPEN
    clock.advance(10)
    with pytest.raises(CircuitOpen):
        cb.call(lambda: "ok")


def test_success_resets_the_failure_count(clock):
    cb = CircuitBreaker("dep", failure_threshold=3, clock=clock)
    for _ in range(2):
        with pytest.raises(Boom):
            cb.call(failing)
    cb.call(lambda: None)
    for _ in range(2):
        with pytest.raises(Boom):
            cb.call(failing)
    assert cb.state is State.CLOSED
