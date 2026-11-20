from prometheus_client import Counter, Gauge, Histogram

REQUESTS_SHED = Counter("lab_requests_shed_total", "Requests rejected by admission control")
RETRIES = Counter("lab_retries_total", "Retries performed", ["dependency"])
RETRY_BUDGET_EXHAUSTED = Counter(
    "lab_retry_budget_exhausted_total", "Retries refused by the retry budget", ["dependency"]
)
BREAKER_STATE = Gauge("lab_circuit_open", "1 while the circuit is open or half-open", ["dependency"])
DEGRADED = Counter("lab_degraded_decisions_total", "Requests served with a degraded fallback", ["reason"])
IDEMPOTENT_REPLAYS = Counter("lab_idempotent_replays_total", "Requests answered from the idempotency store")
DUPLICATE_DELIVERIES = Counter("lab_duplicate_deliveries_total", "Messages whose effect was already applied")
LATENCY = Histogram(
    "lab_request_seconds", "Request latency", ["route"], buckets=(0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2)
)
