"""Game day: drive the running stack through failure experiments and check every expectation.

    python scripts/game_day.py --api http://localhost:8080 --fraud http://localhost:8081 \
        --db postgresql://postgres:lab@localhost:5432/lab [--compose] [--summary results.md --json results.json]

Experiments
  1. baseline        steady load; latency percentiles; every job captured end to end
  2. duplicate storm  50 concurrent POSTs with one Idempotency-Key → exactly one payment
  3. dependency down  fraud service returns 503 → circuit opens, requests degrade to "review", stay fast
  4. overload         slow dependency + 6× the in-flight limit → excess shed with 503 + Retry-After,
                      admitted requests still answered within the deadline
  5. worker SIGKILL   (with --compose) kill the worker mid-processing, restart it → every payment
                      captured exactly once (one ledger row each), nothing lost
Exit code 1 if any expectation fails, so CI goes red.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import httpx
import psycopg


@dataclass
class Report:
    rows: list[dict] = field(default_factory=list)

    def check(self, experiment: str, expectation: str, ok: bool, observed: str) -> None:
        self.rows.append({"experiment": experiment, "expectation": expectation, "ok": bool(ok), "observed": observed})
        print(f"[{'PASS' if ok else 'FAIL'}] {experiment}: {expectation} — {observed}", flush=True)

    @property
    def ok(self) -> bool:
        return all(r["ok"] for r in self.rows)

    def markdown(self) -> str:
        lines = ["## Game day results", "", "| | Experiment | Expectation | Observed |", "|---|---|---|---|"]
        lines += [
            f"| {'✅' if r['ok'] else '❌'} | {r['experiment']} | {r['expectation']} | {r['observed']} |"
            for r in self.rows
        ]
        return "\n".join(lines) + "\n"


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def post(client: httpx.Client, key: str, amount: int = 4200) -> tuple[int, float, dict, httpx.Headers]:
    start = time.perf_counter()
    r = client.post(
        "/payments", json={"customer_id": "gameday", "amount_cents": amount}, headers={"Idempotency-Key": key}
    )
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return r.status_code, time.perf_counter() - start, body, r.headers


def burst(client: httpx.Client, n: int, concurrency: int, prefix: str, key=None):
    with ThreadPoolExecutor(concurrency) as ex:
        return list(ex.map(lambda i: post(client, key or f"{prefix}-{uuid.uuid4()}"), range(n)))


def set_fraud(fraud: httpx.Client, mode: str, latency_ms: int = 20) -> None:
    fraud.post("/admin/mode", params={"mode": mode, "latency_ms": latency_ms}).raise_for_status()


def metric(client: httpx.Client, name: str) -> float:
    total = 0.0
    for line in client.get("/metrics").text.splitlines():
        if line.startswith(name) and not line.startswith("#"):
            total += float(line.rsplit(" ", 1)[1])
    return total


def server_latency_buckets(client: httpx.Client, route: str = "/payments") -> dict[str, float]:
    """Cumulative histogram buckets of server-side request time (excludes client/TCP queueing)."""
    out = {}
    for line in client.get("/metrics").text.splitlines():
        if line.startswith("lab_request_seconds_bucket{") and f'route="{route}"' in line:
            out[line.split('le="')[1].split('"')[0]] = float(line.rsplit(" ", 1)[1])
    return out


def wait_captured(db: str, ids: list[str], timeout: float) -> tuple[int, float]:
    start = time.time()
    while True:
        with psycopg.connect(db) as conn:
            done = conn.execute(
                "SELECT count(*) FROM payments WHERE payment_id = ANY(%s::uuid[]) AND status='captured'", (ids,)
            ).fetchone()[0]
        if done == len(ids) or time.time() - start > timeout:
            return done, time.time() - start
        time.sleep(0.5)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--fraud", default="http://localhost:8081")
    ap.add_argument("--db", default="postgresql://postgres:lab@localhost:5432/lab")
    ap.add_argument("--max-inflight", type=int, default=32)
    ap.add_argument("--breaker-reset", type=float, default=10.0)
    ap.add_argument(
        "--compose", action="store_true", help="run experiments that kill containers (needs docker compose)"
    )
    ap.add_argument("--summary")
    ap.add_argument("--json")
    a = ap.parse_args()

    rep = Report()
    limits = httpx.Limits(max_connections=400, max_keepalive_connections=400)
    api = httpx.Client(base_url=a.api, timeout=10, limits=limits)
    fraud = httpx.Client(base_url=a.fraud, timeout=5)
    set_fraud(fraud, "ok")

    # 1 ── baseline
    res = burst(api, 300, 10, "base")
    lat = [r[1] for r in res if r[0] == 202]
    ok = sum(1 for r in res if r[0] == 202)
    rep.check("1 baseline", "300 requests at concurrency 10 all accepted", ok == 300, f"{ok}/300 × 202")
    rep.check(
        "1 baseline",
        "p99 latency ≤ 400 ms",
        pct(lat, 99) <= 0.4,
        f"p50 {pct(lat, 50) * 1000:.0f} ms · p95 {pct(lat, 95) * 1000:.0f} ms · p99 {pct(lat, 99) * 1000:.0f} ms",
    )
    pending = [r[2]["payment_id"] for r in res if r[0] == 202 and r[2].get("status") == "pending"]
    if a.compose:
        done, took = wait_captured(a.db, pending, 120)
        rep.check(
            "1 baseline",
            "every job captured by the worker (outbox → SQS → worker)",
            done == len(pending),
            f"{done}/{len(pending)} captured in {took:.1f} s",
        )

    # 2 ── duplicate storm
    key = f"dup-{uuid.uuid4()}"
    res = burst(api, 50, 25, "dup", key=key)
    ids = {r[2].get("payment_id") for r in res if r[0] == 202}
    with psycopg.connect(a.db) as conn:
        rows = conn.execute("SELECT count(*) FROM idempotency_keys WHERE key = %s", (key,)).fetchone()[0]
    rep.check(
        "2 duplicate storm",
        "50 concurrent POSTs, one key → one payment, same answer",
        len(ids) == 1 and rows == 1 and all(r[0] == 202 for r in res),
        f"{len(ids)} payment id · {sum(r[3].get('Idempotent-Replayed') == 'true' for r in res)} replays",
    )

    # 3 ── dependency down
    set_fraud(fraud, "down")
    res = burst(api, 100, 10, "down")
    statuses = {r[2].get("status") for r in res if r[0] == 202}
    lat = [r[1] for r in res]
    opened = metric(api, 'lab_circuit_open{dependency="fraud"}')
    rep.check(
        "3 dependency down",
        "requests still accepted, degraded to manual review",
        all(r[0] == 202 for r in res) and statuses == {"review"},
        f"{sum(r[0] == 202 for r in res)}/100 × 202, status {sorted(statuses)}",
    )
    rep.check("3 dependency down", "circuit breaker opened", opened == 1, f"lab_circuit_open = {opened:.0f}")
    rep.check(
        "3 dependency down",
        "fail fast: p99 ≤ 100 ms while open",
        pct(lat[20:], 99) <= 0.1,
        f"p99 {pct(lat[20:], 99) * 1000:.0f} ms (after the circuit opened)",
    )
    set_fraud(fraud, "ok")
    time.sleep(a.breaker_reset + 1)
    s, _, body, _ = post(api, f"recover-{uuid.uuid4()}")
    rep.check(
        "3 dependency down",
        "recovers automatically after the reset window",
        s == 202 and body.get("status") == "pending" and metric(api, 'lab_circuit_open{dependency="fraud"}') == 0,
        f"status {body.get('status')} after {a.breaker_reset:.0f} s",
    )

    # 4 ── overload
    set_fraud(fraud, "ok", latency_ms=200)
    n = a.max_inflight * 6
    shed_before = metric(api, "lab_requests_shed_total")
    hist_before = server_latency_buckets(api)
    res = burst(api, n, n, "overload")
    hist_after = server_latency_buckets(api)
    accepted = [r for r in res if r[0] == 202]
    shed = [r for r in res if r[0] == 503]
    rep.check(
        "4 overload",
        f"{n} simultaneous requests vs limit {a.max_inflight}: excess shed with 503 + Retry-After",
        len(shed) > 0 and all(r[3].get("Retry-After") == "1" for r in shed),
        f"{len(accepted)} accepted · {len(shed)} shed · "
        f"counter +{metric(api, 'lab_requests_shed_total') - shed_before:.0f}",
    )
    within = hist_after.get("2.0", 0) - hist_before.get("2.0", 0)
    total = hist_after.get("+Inf", 0) - hist_before.get("+Inf", 0)
    rep.check(
        "4 overload",
        "admitted requests finish inside the 2 s deadline (server-side)",
        total > 0 and within == total,
        f"{within:.0f}/{total:.0f} within 2 s on the server · client-observed max "
        f"{max((r[1] for r in accepted), default=0) * 1000:.0f} ms incl. connection queueing · "
        f"shed answered in median {statistics.median([r[1] for r in shed]) * 1000 if shed else 0:.0f} ms",
    )
    rep.check(
        "4 overload",
        "no other errors",
        len(accepted) + len(shed) == n,
        f"{n - len(accepted) - len(shed)} other responses",
    )
    set_fraud(fraud, "ok")

    # 5 ── worker killed mid-processing
    if a.compose:
        res = burst(api, 200, 20, "kill")
        ids = [r[2]["payment_id"] for r in res if r[0] == 202 and r[2].get("status") == "pending"]
        time.sleep(2)  # let the worker get going
        subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", "worker"], check=True)
        with psycopg.connect(a.db) as conn:
            mid = conn.execute(
                "SELECT count(*) FROM payments WHERE payment_id = ANY(%s::uuid[]) AND status='captured'", (ids,)
            ).fetchone()[0]
        subprocess.run(["docker", "compose", "up", "-d", "worker"], check=True)
        done, took = wait_captured(a.db, ids, 180)
        with psycopg.connect(a.db) as conn:
            ledger = conn.execute(
                "SELECT count(*), count(DISTINCT payment_id) FROM ledger_entries WHERE payment_id = ANY(%s::uuid[])",
                (ids,),
            ).fetchone()
        rep.check(
            "5 worker SIGKILL",
            "killed mid-batch, restarted: nothing lost",
            done == len(ids),
            f"{mid}/{len(ids)} captured at kill · {done}/{len(ids)} after {took:.0f} s",
        )
        rep.check(
            "5 worker SIGKILL",
            "nothing applied twice (one ledger entry per payment)",
            ledger[0] == ledger[1] == len(ids),
            f"{ledger[0]} ledger rows for {ledger[1]} payments",
        )

    md = rep.markdown()
    print(md)
    if a.summary:
        with open(a.summary, "a") as f:
            f.write(md)
    if a.json:
        with open(a.json, "w") as f:
            json.dump(rep.rows, f, indent=2)
    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())
