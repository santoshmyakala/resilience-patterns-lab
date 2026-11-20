# Resilience patterns lab

[![ci](https://github.com/santoshmyakala/resilience-patterns-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/santoshmyakala/resilience-patterns-lab/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![SQS](https://img.shields.io/badge/SQS-ElasticMQ%20in%20CI-FF9900?logo=amazonsqs&logoColor=white)

Architecture diagrams say "retries", "idempotent" and "circuit breaker". This repository shows the code behind those words, and **tests that prove the behaviour**, using a small payments service: an API, a queue worker and a flaky dependency. It runs against real PostgreSQL, and in CI against real ElasticMQ (an SQS-compatible broker).

It's the application half of [aws-saas-production-platform](https://github.com/santoshmyakala/aws-saas-production-platform). That repository builds the infrastructure (RDS Proxy, SQS + DLQ, canary deploys, SLO alarms); this one implements what the services must do on top of it.

## The incident it's built around

> A worker commits a payment update, then crashes before acknowledging the message. SQS delivers the message again. **Was the customer charged twice?**

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/diagrams/exactly-once-dark.svg">
  <img alt="Exactly-once effects: the API writes the payment, the idempotency key and an outbox row in one transaction; a relay publishes to SQS; the worker captures with the provider using the payment's idempotency key, inserts the ledger entry with ON CONFLICT DO NOTHING in the same transaction and acknowledges only after commit; a crash before acknowledgement causes a redelivery that becomes a no-op; poison messages go to the DLQ after five receives." src="docs/diagrams/exactly-once-light.svg" width="100%">
</picture>

<sub>Animated diagrams generated from <a href="docs/diagrams">docs/diagrams/diagrams.yaml</a> with <a href="https://github.com/santoshmyakala/santoshmyakala/tree/main/tools/archdiagram">archdiagram</a>.</sub>

The delivery guarantee is *at-least-once*, so the design makes the **effect** exactly-once:

| Layer | Mechanism | Code |
|---|---|---|
| API | Client `Idempotency-Key`. A replay returns the stored response; the same key with a different body gets 422; concurrent duplicates create one payment | `lab/payments.py: create_payment` |
| API → queue | **Transactional outbox**: the payment and its job are committed together, then a relay publishes (no dual-write gap) | `publish_outbox` |
| Worker → provider | Capture carries the payment's idempotency key, and the provider dedupes | `capture` |
| Worker → database | Ledger `UNIQUE(payment_id)` + `ON CONFLICT DO NOTHING`, in the same transaction as the status change | `capture` |
| Acknowledgement | Delete the message **only after commit**; on failure, back off the next delivery (exponential + jitter) | `process_message` |
| Poison messages | After five receives SQS moves the message to the DLQ, so healthy work keeps flowing | redrive policy |

`test_crash_after_commit_before_ack_charges_exactly_once` kills the worker at exactly that point. The message comes back with `receive_count == 2`, the provider sees two calls but **one charge**, and the ledger has **one entry**.

## Protecting a request from a failing dependency

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/diagrams/request-protection-dark.svg">
  <img alt="Request protection chain: admission control sheds excess load with 503 and Retry-After, the remaining deadline bounds each call, retries stay inside a budget, a circuit breaker stops calls to a failing fraud service and the request degrades to a manual-review fallback." src="docs/diagrams/request-protection-light.svg" width="100%">
</picture>

| Pattern | Behaviour | Proved by |
|---|---|---|
| **Admission control** | More than `MAX_INFLIGHT` concurrent requests: the excess gets `503` + `Retry-After: 1` immediately instead of queueing. Health checks are never shed | `test_load_shedding_rejects_excess_with_retry_after`, `test_health_endpoints_are_never_shed` |
| **Deadlines** | `X-Request-Deadline-Ms` carries the *remaining* budget hop to hop. Downstream timeouts and Postgres `statement_timeout` are capped by it | `test_deadline.py`, `test_remaining_deadline_is_propagated_downstream`, `test_caller_deadline_is_honoured` |
| **Backoff with full jitter** | `random(0, min(cap, base·2ⁿ))` spreads retries over the whole window | `test_full_jitter_stays_within_exponential_envelope` |
| **Retry budget** | Retries are allowed only up to 10% of recent requests. During an outage, load on the dependency stays ≈ 1.1×, not 3× | `test_retry_budget_caps_retries_at_ten_percent_of_requests` |
| **Circuit breaker** | 5 consecutive failures open it, so calls fail fast without spending a timeout. After 30 s one probe is allowed through: success closes it, failure re-opens it | `test_breaker.py` |
| **Graceful degradation** | If the fraud check is down, the payment is accepted *for manual review*. It isn't rejected, and it isn't captured blindly | `test_outage_degrades_to_review_and_opens_the_circuit` |
| **Readiness ≠ liveness** | `/healthz/ready` checks the database within 250 ms; `/healthz/live` doesn't. The container smoke test verifies readiness fails without a DB | `scripts/smoke-test.sh` |

## Retry amplification, measured

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/diagrams/retry-amplification-dark.svg">
  <img alt="Retry amplification: when the client, service A and service B each retry three times, one request becomes 27 calls to the failing service C; when only the edge retries within a budget, it is about 1.1 calls." src="docs/diagrams/retry-amplification-light.svg" width="100%">
</picture>

From `tests/test_amplification.py`, with service C down:

| Retry policy | Calls to C for 1 request | Calls to C for 500 requests |
|---|---:|---:|
| Every layer retries 3× | **27** | 13,500 |
| Only the edge retries 3× | 3 | 1,500 |
| Edge retries within a 10% budget | — | **≤ 551** |

## Tests

Two queue implementations run the same worker tests:
- an in-process double with SQS semantics (visibility timeout, receive count, redrive);
- **ElasticMQ in CI**, the real SQS API.

The database is always real PostgreSQL.

```
test_amplification.py   27× amplification, edge-only retries, retry budget under outage
test_retry.py           full-jitter bounds, attempts, non-retryable errors, deadline-aware sleeping, budget
test_breaker.py         open after threshold, fail fast, single half-open probe, re-open, reset on success
test_deadline.py        nested deadlines only shrink, per-call timeout from remaining budget, propagation
test_fraud_client.py    breaker + retries + deadline around a real HTTP client (httpx MockTransport)
test_api.py             idempotent POST, 422 on key reuse, 8 concurrent duplicates → 1 payment,
                        load shedding with Retry-After, health never shed, caller deadline honoured
test_worker.py          outbox → capture, crash-after-commit → exactly once, double publish harmless,
                        poison → DLQ after 5, failed job backed off and retried
```

```bash
pip install -e ".[dev]"
```

```bash
docker run -d --rm -p 5432:5432 -e POSTGRES_PASSWORD=lab -e POSTGRES_DB=lab postgres:16-alpine
```

```bash
DATABASE_URL=postgresql://postgres:lab@localhost:5432/lab pytest -v
```

Add `SQS_ENDPOINT=http://localhost:9324` (with `softwaremill/elasticmq-native` running) to repeat the worker tests against the SQS API.

## Play with it

```bash
docker compose up --build
```

```bash
curl -s localhost:8080/payments -H 'Idempotency-Key: demo-0001' -H 'content-type: application/json' \
     -d '{"customer_id":"acme","amount_cents":4200}'
```

Break the dependency and watch payments degrade to `review` while `lab_circuit_open` goes to 1:

```bash
curl -s -X POST 'localhost:8081/admin/mode?mode=down'
```

```bash
curl -s localhost:8080/metrics | grep -E 'lab_(circuit|degraded|retries|requests_shed)'
```

## Design notes

- **The provider call happens inside the worker's transaction**, while it holds the payment row lock (`SELECT … FOR UPDATE`). That serialises concurrent duplicate deliveries of the same payment. The trade-off is a held lock during an external call, bounded by the call's timeout. For high-contention workloads, a two-phase approach is better: record the attempt, call, then confirm.
- **Migrations run as their own step** (`python -m lab.db`), never at application start-up. A fleet of starting tasks shouldn't race to alter the schema.
- **One retry layer.** Only the client edge retries. Internal calls use `NO_RETRY` or a budgeted policy, which is what keeps the amplification table above linear.

## Credits

Inspired by the timeout, retry and asynchronous-processing chapters of a public AWS architecture infographic series by [@veriqta](https://www.instagram.com/veriqta/). All code, tests and write-ups here are original.

## Licence

MIT
