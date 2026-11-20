"""HTTP API: deadline propagation, load shedding, idempotent POST, dependency-aware readiness."""

from __future__ import annotations

import os
import time

import psycopg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from psycopg_pool import ConnectionPool, PoolTimeout
from pydantic import BaseModel, Field

from . import db
from . import deadline as dl
from .admission import ConcurrencyLimiter, Overloaded
from .dependencies import FraudClient
from .metrics import LATENCY, REQUESTS_SHED
from .payments import IdempotencyConflict, create_payment

UNSHED = ("/healthz", "/metrics")


class PaymentIn(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(gt=0, le=10_000_000)


def create_app(
    pool: ConnectionPool,
    fraud: FraudClient,
    *,
    max_in_flight: int | None = None,
    default_deadline_ms: int | None = None,
) -> FastAPI:
    app = FastAPI(title="resilience-patterns-lab")
    limiter = ConcurrencyLimiter(max_in_flight or int(os.environ.get("MAX_INFLIGHT", "64")))
    default_ms = default_deadline_ms or int(os.environ.get("REQUEST_DEADLINE_MS", "2000"))
    app.state.limiter = limiter

    @app.middleware("http")
    async def resilience_middleware(request: Request, call_next):
        if request.url.path.startswith(UNSHED):
            return await call_next(request)
        # Honour the caller's remaining budget, but never exceed our own maximum.
        try:
            budget_ms = min(default_ms, int(request.headers.get(dl.HEADER, default_ms)))
        except ValueError:
            budget_ms = default_ms
        start = time.perf_counter()
        try:
            with limiter.slot(), dl.deadline(budget_ms / 1000):
                response = await call_next(request)
        except Overloaded:
            REQUESTS_SHED.inc()
            return JSONResponse({"error": "overloaded"}, status_code=503, headers={"Retry-After": "1"})
        LATENCY.labels(request.scope.get("route").path if request.scope.get("route") else "unmatched").observe(
            time.perf_counter() - start
        )
        return response

    @app.exception_handler(dl.DeadlineExceeded)
    async def deadline_exceeded(_: Request, exc: dl.DeadlineExceeded):
        return JSONResponse({"error": "deadline exceeded"}, status_code=504)

    @app.post("/payments", status_code=202)
    def post_payment(body: PaymentIn, idempotency_key: str = Header(min_length=8, max_length=128)):
        try:
            result = create_payment(pool, fraud, idempotency_key, body.model_dump())
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        headers = {"Idempotent-Replayed": "true"} if result.replayed else {}
        return JSONResponse(result.body, status_code=result.status_code, headers=headers)

    @app.get("/payments/{payment_id}")
    def get_payment(payment_id: str):
        with db.transaction(pool) as conn:
            row = conn.execute(
                "SELECT status, amount_cents FROM payments WHERE payment_id = %s", (payment_id,)
            ).fetchone()
        if row is None:
            raise HTTPException(404)
        return {"payment_id": payment_id, "status": row[0], "amount_cents": row[1]}

    @app.get("/healthz/live")
    def live():
        return {"status": "ok"}

    @app.get("/healthz/ready")
    def ready():
        # Ready means "can serve real requests": the database answers within a tight budget.
        try:
            with dl.deadline(0.25), db.transaction(pool, default_timeout_ms=200) as conn:
                conn.execute("SELECT 1")
        except (psycopg.Error, PoolTimeout, dl.DeadlineExceeded):
            return JSONResponse({"status": "not ready", "dependency": "database"}, status_code=503)
        return {"status": "ready"}

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def main() -> FastAPI:  # uvicorn --factory lab.api:main
    return create_app(db.make_pool(), FraudClient(os.environ.get("FRAUD_URL", "http://localhost:8081")))
