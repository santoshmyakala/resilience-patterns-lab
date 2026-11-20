"""Queue worker: relay the outbox, then consume jobs. Graceful shutdown finishes the current message."""

from __future__ import annotations

import logging
import os
import signal
import time

from . import db
from .dependencies import FakePaymentProvider
from .payments import process_message, publish_outbox
from .queue import Queue, SqsQueue

log = logging.getLogger("worker")


def run(queue: Queue, pool, provider, *, stop=lambda: False, wait_seconds: float = 10) -> None:
    while not stop():
        publish_outbox(pool, queue)
        for msg in queue.receive(max_messages=10, wait_seconds=wait_seconds):
            try:
                process_message(pool, queue, provider, msg)
            except Exception:  # noqa: BLE001 - any failure leaves the message for redelivery
                log.exception("job failed (receive %s); it will be retried, then dead-lettered", msg.receive_count)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    endpoint = os.environ.get("SQS_ENDPOINT")
    queue = (
        SqsQueue.create(os.environ.get("QUEUE_NAME", "jobs"), endpoint_url=endpoint)
        if endpoint
        else SqsQueue(os.environ["QUEUE_URL"])
    )
    pool = db.make_pool()
    while True:
        try:
            run(queue, pool, FakePaymentProvider(), stop=lambda: stopping)
            return
        except Exception:  # noqa: BLE001 - dependency outage: back off, don't crash-loop
            log.exception("worker loop error")
            time.sleep(5)


if __name__ == "__main__":
    main()
