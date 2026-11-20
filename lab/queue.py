"""Queue interface with two implementations sharing SQS semantics.

* ``SqsQueue``   real Amazon SQS (or ElasticMQ in CI / docker compose) via boto3.
* ``MemoryQueue`` in-process test double that models what matters for correctness:
  at-least-once delivery, visibility timeout, ApproximateReceiveCount and dead-letter redrive.
"""

from __future__ import annotations

import itertools
import json
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass
class Message:
    message_id: str
    receipt_handle: str
    body: dict
    receive_count: int


class Queue(Protocol):
    def send(self, body: dict) -> str: ...
    def receive(self, max_messages: int = 10, wait_seconds: float = 0.0) -> list[Message]: ...
    def delete(self, msg: Message) -> None: ...
    def change_visibility(self, msg: Message, seconds: int) -> None: ...
    def dlq_depth(self) -> int: ...


class MemoryQueue:
    def __init__(
        self, visibility_timeout: float = 30.0, max_receive_count: int = 5, clock: Callable[[], float] = time.monotonic
    ):
        self.visibility_timeout = visibility_timeout
        self.max_receive_count = max_receive_count
        self._clock = clock
        self._lock = threading.Lock()
        self._seq = itertools.count()
        # message_id -> [body, visible_at, receive_count, current_receipt]
        self._messages: dict[str, list] = {}
        self.dead_letters: list[dict] = []

    def send(self, body: dict) -> str:
        mid = str(uuid.uuid4())
        with self._lock:
            self._messages[mid] = [json.loads(json.dumps(body)), self._clock(), 0, None, next(self._seq)]
        return mid

    def receive(self, max_messages: int = 10, wait_seconds: float = 0.0) -> list[Message]:
        out: list[Message] = []
        now = self._clock()
        with self._lock:
            for mid, rec in sorted(self._messages.items(), key=lambda kv: kv[1][4]):
                if len(out) >= max_messages:
                    break
                if rec[1] > now:
                    continue  # in flight with another consumer
                if rec[2] >= self.max_receive_count:
                    # SQS redrive: a message received maxReceiveCount times moves to the DLQ
                    self.dead_letters.append(rec[0])
                    del self._messages[mid]
                    continue
                rec[2] += 1
                rec[1] = now + self.visibility_timeout
                rec[3] = str(uuid.uuid4())
                out.append(Message(mid, rec[3], json.loads(json.dumps(rec[0])), rec[2]))
        return out

    def delete(self, msg: Message) -> None:
        with self._lock:
            rec = self._messages.get(msg.message_id)
            if rec and rec[3] == msg.receipt_handle:  # a stale receipt cannot delete a redelivered copy
                del self._messages[msg.message_id]

    def change_visibility(self, msg: Message, seconds: int) -> None:
        with self._lock:
            rec = self._messages.get(msg.message_id)
            if rec and rec[3] == msg.receipt_handle:
                rec[1] = self._clock() + seconds

    def depth(self) -> int:
        with self._lock:
            return len(self._messages)

    def dlq_depth(self) -> int:
        return len(self.dead_letters)


class SqsQueue:
    def __init__(self, queue_url: str, dlq_url: str | None = None, client=None):
        import boto3

        self.queue_url = queue_url
        self.dlq_url = dlq_url
        self._sqs = client or boto3.client("sqs")

    @classmethod
    def create(
        cls,
        name: str,
        *,
        endpoint_url: str | None = None,
        region: str = "us-east-1",
        visibility_timeout: int = 30,
        max_receive_count: int = 5,
    ) -> SqsQueue:
        """Create queue + DLQ with a redrive policy (used by tests and docker compose)."""
        import boto3

        sqs = boto3.client("sqs", endpoint_url=endpoint_url, region_name=region)
        dlq_url = sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        url = sqs.create_queue(
            QueueName=name,
            Attributes={
                "VisibilityTimeout": str(visibility_timeout),
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receive_count)}
                ),
            },
        )["QueueUrl"]
        return cls(url, dlq_url, sqs)

    def send(self, body: dict) -> str:
        return self._sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(body))["MessageId"]

    def receive(self, max_messages: int = 10, wait_seconds: float = 0.0) -> list[Message]:
        resp = self._sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=int(wait_seconds),
            AttributeNames=["ApproximateReceiveCount"],  # also understood by ElasticMQ
        )
        return [
            Message(
                m["MessageId"],
                m["ReceiptHandle"],
                json.loads(m["Body"]),
                int(m.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
            )
            for m in resp.get("Messages", [])
        ]

    def delete(self, msg: Message) -> None:
        self._sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=msg.receipt_handle)

    def change_visibility(self, msg: Message, seconds: int) -> None:
        self._sqs.change_message_visibility(
            QueueUrl=self.queue_url, ReceiptHandle=msg.receipt_handle, VisibilityTimeout=seconds
        )

    def dlq_depth(self) -> int:
        if not self.dlq_url:
            return 0
        attrs = self._sqs.get_queue_attributes(QueueUrl=self.dlq_url, AttributeNames=["ApproximateNumberOfMessages"])
        return int(attrs["Attributes"]["ApproximateNumberOfMessages"])
