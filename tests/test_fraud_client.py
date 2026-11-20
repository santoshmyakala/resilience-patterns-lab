import httpx

from lab import deadline as dl
from lab.breaker import CircuitBreaker, State
from lab.dependencies import FraudClient
from lab.retry import RetryPolicy


def client(handler, clock, **kw):
    http = httpx.Client(base_url="http://fraud", transport=httpx.MockTransport(handler))
    breaker = CircuitBreaker("fraud", failure_threshold=3, reset_after=30, clock=clock)
    return FraudClient("http://fraud", http, breaker=breaker, retry=RetryPolicy(attempts=1), **kw)


def test_healthy_dependency_decides(clock):
    fc = client(lambda req: httpx.Response(200, json={"decision": "approve"}), clock)
    assert fc.decide("c1", 1000) == "approve"


def test_outage_degrades_to_review_and_opens_the_circuit(clock):
    hits = []

    def down(req):
        hits.append(1)
        return httpx.Response(503)

    fc = client(down, clock)
    decisions = [fc.decide("c1", 1000) for _ in range(10)]
    assert decisions == ["review"] * 10  # the request still succeeds, held for manual review
    assert len(hits) == 3  # after 3 failures the breaker stops calling the dependency at all
    assert fc.breaker.state is State.OPEN


def test_recovers_after_reset_window(clock):
    status = {"code": 503}
    fc = client(lambda req: httpx.Response(status["code"], json={"decision": "approve"}), clock)
    for _ in range(3):
        fc.decide("c1", 1000)
    status["code"] = 200
    clock.advance(30)
    assert fc.decide("c1", 1000) == "approve"
    assert fc.breaker.state is State.CLOSED


def test_remaining_deadline_is_propagated_downstream(clock):
    seen = {}

    def handler(req):
        seen["deadline"] = req.headers.get(dl.HEADER)
        return httpx.Response(200, json={"decision": "approve"})

    fc = client(handler, clock)
    import time

    dl.set_clock(clock)
    try:
        with dl.deadline(2.0):
            clock.advance(0.5)
            fc.decide("c1", 1000)
    finally:
        dl.set_clock(time.monotonic)
    assert seen["deadline"] == "1500"


def test_timeouts_are_retryable_and_degrade(clock):
    def slow(req):
        raise httpx.ReadTimeout("slow", request=req)

    assert client(slow, clock).decide("c1", 1000) == "review"
