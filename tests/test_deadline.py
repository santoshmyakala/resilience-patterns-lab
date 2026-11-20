import time

import pytest

from lab import deadline as dl


@pytest.fixture(autouse=True)
def fake_clock(clock):
    dl.set_clock(clock)
    yield clock
    dl.set_clock(time.monotonic)


def test_nested_deadlines_only_shrink(fake_clock):
    with dl.deadline(2.0):
        with dl.deadline(10.0):
            assert dl.remaining() == pytest.approx(2.0)
        with dl.deadline(0.5):
            assert dl.remaining() == pytest.approx(0.5)


def test_downstream_timeout_is_the_smaller_of_its_limit_and_the_budget(fake_clock):
    assert dl.timeout_for(0.3) == 0.3  # no deadline active
    with dl.deadline(2.0):
        assert dl.timeout_for(0.3) == pytest.approx(0.3)
        fake_clock.advance(1.9)
        assert dl.timeout_for(0.3) == pytest.approx(0.1)
        assert dl.header_value() == "100"  # propagated to the next hop in milliseconds


def test_spent_budget_stops_new_work(fake_clock):
    with dl.deadline(1.0):
        fake_clock.advance(1.5)
        with pytest.raises(dl.DeadlineExceeded):
            dl.check()
        with pytest.raises(dl.DeadlineExceeded):
            dl.timeout_for(0.3)
