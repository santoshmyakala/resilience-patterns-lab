"""Postgres access: a bounded connection pool and the schema.

The pool size is a hard cap per process (``DB_POOL_MAX``): scaling out adds processes, never
unbounded connections per process. Every transaction gets a statement timeout derived from the
request deadline, so a slow query cannot outlive the request that issued it.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool

from . import deadline as dl

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    payment_id   uuid PRIMARY KEY,
    customer_id  text NOT NULL,
    amount_cents integer NOT NULL CHECK (amount_cents > 0),
    status       text NOT NULL CHECK (status IN ('pending', 'review', 'captured')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    captured_at  timestamptz
);
-- API idempotency: one row per client-supplied Idempotency-Key
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          text PRIMARY KEY,
    request_hash text NOT NULL,
    status       text NOT NULL CHECK (status IN ('in_progress', 'completed')),
    response     jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);
-- Transactional outbox: the job is recorded in the same transaction as the payment
CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial PRIMARY KEY,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
-- The money movement. UNIQUE(payment_id) makes the effect exactly-once even under redelivery.
CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_id     bigserial PRIMARY KEY,
    payment_id   uuid NOT NULL UNIQUE REFERENCES payments (payment_id),
    amount_cents integer NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
"""


def database_url() -> str:
    return os.environ.get("DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/lab")


def make_pool(url: str | None = None, max_size: int | None = None, migrate: bool = False) -> ConnectionPool:
    pool = ConnectionPool(
        url or database_url(),
        min_size=1,
        max_size=max_size or int(os.environ.get("DB_POOL_MAX", "10")),
        timeout=float(os.environ.get("DB_POOL_TIMEOUT", "5")),  # fail fast when saturated
        open=True,
    )
    if migrate:
        with pool.connection() as conn:
            conn.execute(SCHEMA)
    return pool


def migrate() -> None:
    """Schema migration as its own step (CI, compose, a one-off ECS task) — not on app start-up,
    so a fleet of starting tasks never races to alter the schema."""
    with psycopg.connect(database_url()) as conn:
        conn.execute(SCHEMA)


if __name__ == "__main__":
    migrate()


@contextmanager
def transaction(pool: ConnectionPool, default_timeout_ms: int = 1000) -> Iterator[psycopg.Connection]:
    budget_ms = int(dl.timeout_for(default_timeout_ms / 1000) * 1000)
    with pool.connection(timeout=dl.timeout_for(5.0)) as conn, conn.transaction():
        # set_config(..., is_local => true) == SET LOCAL, but parameterised
        conn.execute("SELECT set_config('statement_timeout', %s, true)", (str(max(budget_ms, 1)),))
        yield conn
