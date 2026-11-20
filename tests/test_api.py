import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from lab.api import create_app

pytestmark = pytest.mark.integration


class StubFraud:
    def __init__(self, decision="approve", gate: threading.Event | None = None):
        self.decision, self.gate, self.calls = decision, gate, 0

    def decide(self, customer_id, amount_cents):
        self.calls += 1
        if self.gate:
            self.gate.wait(5)
        return self.decision


def body(amount=1500):
    return {"customer_id": "acme", "amount_cents": amount}


def test_post_is_idempotent(clean):
    api = TestClient(create_app(clean, StubFraud()))
    first = api.post("/payments", json=body(), headers={"Idempotency-Key": "order-0001"})
    again = api.post("/payments", json=body(), headers={"Idempotency-Key": "order-0001"})
    assert first.status_code == again.status_code == 202
    assert again.json() == first.json() and again.headers["Idempotent-Replayed"] == "true"
    with clean.connection() as conn:
        assert conn.execute("SELECT count(*) FROM payments").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1


def test_key_reuse_with_a_different_body_is_rejected(clean):
    api = TestClient(create_app(clean, StubFraud()))
    api.post("/payments", json=body(1500), headers={"Idempotency-Key": "order-0002"})
    r = api.post("/payments", json=body(9999), headers={"Idempotency-Key": "order-0002"})
    assert r.status_code == 422


def test_concurrent_duplicates_create_exactly_one_payment(clean):
    api = TestClient(create_app(clean, StubFraud()))
    with ThreadPoolExecutor(8) as ex:
        results = list(
            ex.map(lambda _: api.post("/payments", json=body(), headers={"Idempotency-Key": "order-0003"}), range(8))
        )
    assert {r.status_code for r in results} == {202}
    assert len({r.json()["payment_id"] for r in results}) == 1
    with clean.connection() as conn:
        assert conn.execute("SELECT count(*) FROM payments").fetchone()[0] == 1


def test_degraded_fraud_check_holds_payment_for_review_without_a_job(clean):
    api = TestClient(create_app(clean, StubFraud(decision="review")))
    r = api.post("/payments", json=body(), headers={"Idempotency-Key": "order-0004"})
    assert r.status_code == 202 and r.json()["status"] == "review"
    with clean.connection() as conn:
        assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


def test_load_shedding_rejects_excess_with_retry_after(clean):
    gate = threading.Event()
    api = TestClient(create_app(clean, StubFraud(gate=gate), max_in_flight=2))
    with ThreadPoolExecutor(6) as ex:
        futures = [
            ex.submit(api.post, "/payments", json=body(), headers={"Idempotency-Key": f"order-shed-{i}"})
            for i in range(6)
        ]
        # wait until the two admitted requests are inside, then let them finish
        for _ in range(100):
            if api.app.state.limiter.in_flight == 2 and sum(f.done() for f in futures) == 4:
                break
            threading.Event().wait(0.05)
        gate.set()
        results = [f.result() for f in futures]
    codes = sorted(r.status_code for r in results)
    assert codes == [202, 202, 503, 503, 503, 503]
    assert all(r.headers.get("Retry-After") == "1" for r in results if r.status_code == 503)


def test_health_endpoints_are_never_shed(clean):
    gate = threading.Event()
    api = TestClient(create_app(clean, StubFraud(gate=gate), max_in_flight=1))
    with ThreadPoolExecutor(1) as ex:
        f = ex.submit(api.post, "/payments", json=body(), headers={"Idempotency-Key": "order-busy-1"})
        for _ in range(100):
            if api.app.state.limiter.in_flight == 1:
                break
            threading.Event().wait(0.05)
        assert api.get("/healthz/ready").status_code == 200
        gate.set()
        f.result()


def test_caller_deadline_is_honoured(clean):
    api = TestClient(create_app(clean, StubFraud()))
    r = api.post("/payments", json=body(), headers={"Idempotency-Key": "order-0005", "X-Request-Deadline-Ms": "0"})
    assert r.status_code == 504
