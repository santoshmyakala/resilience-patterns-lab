import os
import time

import psycopg
import pytest

from lab import db
from lab.queue import MemoryQueue, SqsQueue


class FakeClock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock():
    return FakeClock()


def _db_available() -> bool:
    try:
        with psycopg.connect(db.database_url(), connect_timeout=2):
            return True
    except psycopg.OperationalError:
        return False


@pytest.fixture(scope="session")
def pool():
    if not _db_available():
        if os.environ.get("CI"):
            pytest.fail("Postgres is required in CI")
        pytest.skip("Postgres not available (set DATABASE_URL)")
    p = db.make_pool(max_size=10, migrate=True)
    yield p
    p.close()


@pytest.fixture
def clean(pool):
    with pool.connection() as conn:
        conn.execute("TRUNCATE ledger_entries, outbox, idempotency_keys, payments RESTART IDENTITY CASCADE")
    return pool


VISIBILITY = 1  # seconds; short so redelivery tests run fast against a real broker


class QueueHarness:
    """Gives tests one way to 'wait until in-flight messages become visible again'."""

    def __init__(self, kind: str, clock: FakeClock):
        self.kind = kind
        if kind == "memory":
            self.queue = MemoryQueue(visibility_timeout=VISIBILITY, max_receive_count=5, clock=clock)
            self._clock = clock
        else:
            name = f"jobs-{time.time_ns()}"
            self.queue = SqsQueue.create(
                name, endpoint_url=os.environ["SQS_ENDPOINT"], visibility_timeout=VISIBILITY, max_receive_count=5
            )

    def expire_visibility(self) -> None:
        if self.kind == "memory":
            self._clock.advance(VISIBILITY + 0.1)
        else:
            time.sleep(VISIBILITY + 0.5)

    def receive(self):
        # SQS may need a moment after visibility expiry; poll briefly.
        for _ in range(10):
            msgs = self.queue.receive(max_messages=10, wait_seconds=1 if self.kind == "sqs" else 0)
            if msgs:
                return msgs
            if self.kind == "memory":
                return []
        return []


QUEUE_KINDS = ["memory"] + (["sqs"] if os.environ.get("SQS_ENDPOINT") else [])


@pytest.fixture(params=QUEUE_KINDS)
def harness(request, clock):
    return QueueHarness(request.param, clock)
