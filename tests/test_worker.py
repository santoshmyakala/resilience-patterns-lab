"""The scenario: a worker commits a payment capture, then crashes before acknowledging the message.
SQS redelivers it. The customer must still be charged exactly once."""

import uuid

import pytest

from lab.dependencies import FakePaymentProvider
from lab.payments import SimulatedCrash, create_payment, process_message, publish_outbox

pytestmark = pytest.mark.integration


class Approve:
    def decide(self, *_):
        return "approve"


def new_payment(pool, key="order-1"):
    return create_payment(pool, Approve(), key, {"customer_id": "acme", "amount_cents": 4200}).body["payment_id"]


def counts(pool, payment_id):
    with pool.connection() as conn:
        ledger = conn.execute("SELECT count(*) FROM ledger_entries WHERE payment_id = %s", (payment_id,)).fetchone()[0]
        status = conn.execute("SELECT status FROM payments WHERE payment_id = %s", (payment_id,)).fetchone()[0]
    return ledger, status


def crash():
    raise SimulatedCrash("worker killed after commit, before ack")


def test_happy_path_outbox_to_capture(clean, harness):
    pid = new_payment(clean)
    assert publish_outbox(clean, harness.queue) == 1
    assert publish_outbox(clean, harness.queue) == 0  # already published
    provider = FakePaymentProvider()
    [msg] = harness.receive()
    process_message(clean, harness.queue, provider, msg)
    assert counts(clean, pid) == (1, "captured")
    harness.expire_visibility()
    assert harness.queue.receive(wait_seconds=1 if harness.kind == "sqs" else 0) == []  # acknowledged


def test_crash_after_commit_before_ack_charges_exactly_once(clean, harness):
    pid = new_payment(clean)
    publish_outbox(clean, harness.queue)
    provider = FakePaymentProvider()

    [msg] = harness.receive()
    with pytest.raises(SimulatedCrash):
        process_message(clean, harness.queue, provider, msg, after_commit=crash)
    assert counts(clean, pid) == (1, "captured")  # effect committed, message NOT deleted

    harness.expire_visibility()
    [again] = harness.receive()  # at-least-once delivery: it comes back
    assert again.receive_count == 2
    process_message(clean, harness.queue, provider, again)

    assert counts(clean, pid) == (1, "captured")  # still one ledger entry
    assert provider.calls == 2 and len(provider.charges) == 1  # provider deduped by idempotency key
    harness.expire_visibility()
    assert harness.queue.receive(wait_seconds=1 if harness.kind == "sqs" else 0) == []


def test_duplicate_messages_from_a_double_publish_are_harmless(clean, harness):
    pid = new_payment(clean)
    harness.queue.send({"payment_id": pid})
    harness.queue.send({"payment_id": pid})  # e.g. relay crashed between send and marking published
    provider = FakePaymentProvider()
    msgs = harness.receive()
    while len(msgs) < 2:
        msgs += harness.receive()
    for m in msgs:
        process_message(clean, harness.queue, provider, m)
    assert counts(clean, pid) == (1, "captured") and len(provider.charges) == 1


def test_poison_message_is_isolated_in_the_dead_letter_queue(clean, harness):
    harness.queue.send({"payment_id": "not-a-uuid"})
    provider = FakePaymentProvider()
    for attempt in range(1, 6):
        msgs = harness.receive()
        assert len(msgs) == 1 and msgs[0].receive_count == attempt
        with pytest.raises(ValueError):
            process_message(clean, harness.queue, provider, msgs[0])
        # process_message backed the message off (2 s, 4 s, …); skip the wait for the next attempt
        harness.queue.change_visibility(msgs[0], 0)
    assert harness.receive() == []
    assert harness.queue.dlq_depth() == 1  # moved after 5 failed receives; healthy work keeps flowing


def test_failed_job_is_backed_off_then_retried_not_dropped(clean, harness):
    harness.queue.send({"payment_id": str(uuid.uuid4())})
    [msg] = harness.receive()
    with pytest.raises(LookupError):
        process_message(clean, harness.queue, FakePaymentProvider(), msg)
    harness.queue.change_visibility(msg, 0)  # skip the backoff wait
    [again] = harness.receive()
    assert again.receive_count == 2 and harness.queue.dlq_depth() == 0


def test_statement_timeout_follows_the_request_deadline(clean):
    from lab import db
    from lab import deadline as dl

    with dl.deadline(0.3), db.transaction(clean) as conn:
        assert conn.execute("SHOW statement_timeout").fetchone()[0] in {"300ms", "299ms", "298ms"}
    with db.transaction(clean) as conn:
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "1s"  # default when no deadline
