"""Toy fraud-check dependency with switchable failure modes (for the local demo)."""

import os
import random
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="fraud-sim")
state = {"mode": os.environ.get("FRAUD_MODE", "ok"), "latency_ms": 20, "error_rate": 0.0}


@app.post("/admin/mode")
def set_mode(mode: str = "ok", latency_ms: int = 20, error_rate: float = 0.0):
    state.update(mode=mode, latency_ms=latency_ms, error_rate=error_rate)
    return state


@app.post("/check")
def check(body: dict):
    time.sleep(state["latency_ms"] / 1000)
    if state["mode"] == "down" or random.random() < state["error_rate"]:
        return JSONResponse({"error": "unavailable"}, status_code=503)
    return {"decision": "review" if body.get("amount_cents", 0) > 500_000 else "approve"}
