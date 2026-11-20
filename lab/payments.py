"""Payment intake (API side) and capture (worker side).

Intake  : Idempotency-Key per request; payment row + outbox row + stored response in ONE transaction.
Relay   : publishes outbox rows to the queue (at-least-once; duplicates are harmless).
Capture : provider call with the payment's idempotency key, then ledger insert guarded by
          UNIQUE(payment_id), commit, and only then acknowledge (delete) the message.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from . import db
from .dependencies import FakePaymentProvider, FraudClient
from .metrics import DUPLICATE_DELIVERIES, IDEMPOTENT_REPLAYS
from .queue import Message, Queue
from .retry import backoff_delay


class _LostRace(Exception):
    pass


class IdempotencyConflict(Exception):
    """Same Idempotency-Key reused with a different request body."""


@dataclass
class Result:
    status_code: int
    body: dict
    replayed: bool = False


def request_hash(body: dict) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _lookup(pool: ConnectionPool, key: str) -> tuple[str, dict] | None:
    with db.transaction(pool) as conn:
        row = conn.execute("SELECT request_hash, response FROM idempotency_keys WHERE key = %s", (key,)).fetchone()
    return None if row is None else (row[0], row[1])


def create_payment(pool: ConnectionPool, fraud: FraudClient, key: str, body: dict) -> Result:
    h = request_hash(body)
    if (found := _lookup(pool, key)) is not None:
        return _replay(found, h)

    decision = fraud.decide(body["customer_id"], body["amount_cents"])  # outside the transaction
    payment_id = str(uuid.uuid4())
    status = "pending" if decision == "approve" else "review"
    response = {"payment_id": payment_id, "status": status}

    try:
        with db.transaction(pool) as conn:
            claimed = conn.execute(
                "INSERT INTO idempotency_keys (key, request_hash, status, response) VALUES (%s, %s, 'completed', %s) "
                "ON CONFLICT (key) DO NOTHING RETURNING key",
                (key, h, Jsonb(response)),
            ).fetchone()
            if claimed is None:
                # A concurrent request with the same key won the race (Postgres waited for it to commit).
                raise _LostRace
            conn.execute(
                "INSERT INTO payments (payment_id, customer_id, amount_cents, status) VALUES (%s, %s, %s, %s)",
                (payment_id, body["customer_id"], body["amount_cents"], status),
            )
            if status == "pending":
                conn.execute("INSERT INTO outbox (payload) VALUES (%s)", (Jsonb({"payment_id": payment_id}),))
    except _LostRace:
        return _replay(_lookup(pool, key), h)
    return Result(202, response)


def _replay(found: tuple[str, dict] | None, h: str) -> Result:
    assert found is not None
    stored_hash, response = found
    if stored_hash != h:
        raise IdempotencyConflict("Idempotency-Key was already used with a different request body")
    IDEMPOTENT_REPLAYS.inc()
    return Result(202, response, replayed=True)


def publish_outbox(pool: ConnectionPool, queue: Queue, batch: int = 100) -> int:
    """Move committed jobs to the queue. A crash between send and UPDATE re-sends: that's fine,
    the consumer is idempotent."""
    with db.transaction(pool, default_timeout_ms=5000) as conn:
        rows = conn.execute(
            "SELECT id, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
            (batch,),
        ).fetchall()
        for oid, payload in rows:
            queue.send(payload)
            conn.execute("UPDATE outbox SET published_at = now() WHERE id = %s", (oid,))
    return len(rows)


class SimulatedCrash(Exception):
    """Used by tests to kill a worker at a precise point."""


def capture(
    pool: ConnectionPool, provider: FakePaymentProvider, payment_id: str, after_commit: Callable[[], None] | None = None
) -> bool:
    """Apply the capture exactly once. Returns False if it had already been applied."""
    uuid.UUID(payment_id)  # poison-message guard: malformed payloads fail every attempt → DLQ
    with db.transaction(pool, default_timeout_ms=5000) as conn:
        row = conn.execute(
            "SELECT amount_cents FROM payments WHERE payment_id = %s FOR UPDATE", (payment_id,)
        ).fetchone()
        if row is None:
            raise LookupError(f"payment {payment_id} not found")
        provider.capture(idempotency_key=payment_id, amount_cents=row[0])  # PSP dedupes by key
        applied = conn.execute(
            "INSERT INTO ledger_entries (payment_id, amount_cents) VALUES (%s, %s) "
            "ON CONFLICT (payment_id) DO NOTHING RETURNING entry_id",
            (payment_id, row[0]),
        ).fetchone()
        conn.execute(
            "UPDATE payments SET status = 'captured', captured_at = now() WHERE payment_id = %s AND status = 'pending'",
            (payment_id,),
        )
    if after_commit:
        after_commit()  # e.g. the process dies here: committed, not yet acknowledged
    if applied is None:
        DUPLICATE_DELIVERIES.inc()
    return applied is not None


def process_message(
    pool: ConnectionPool,
    queue: Queue,
    provider: FakePaymentProvider,
    msg: Message,
    after_commit: Callable[[], None] | None = None,
) -> None:
    try:
        capture(pool, provider, msg.body["payment_id"], after_commit=after_commit)
    except SimulatedCrash:
        raise
    except Exception:  # noqa: BLE001 - re-raised after scheduling a backed-off redelivery
        # Leave the message on the queue; back off its next delivery so failures don't hot-loop.
        queue.change_visibility(msg, int(max(1, backoff_delay(msg.receive_count, base=2, cap=60))))
        raise
    queue.delete(msg)  # acknowledge only after the effect is durable
