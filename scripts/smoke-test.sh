#!/usr/bin/env bash
# Container smoke test with no database: liveness passes, readiness correctly reports the missing
# dependency, input validation and metrics work, and the process is not root.
set -euo pipefail
: "${IMAGE:?IMAGE must be set}"
docker run -d --name lab-smoke -p 8080:8080 -e DATABASE_URL=postgresql://nobody@127.0.0.1:1/none "$IMAGE" >/dev/null
trap 'docker rm -f lab-smoke >/dev/null' EXIT
for _ in $(seq 1 30); do curl -fsS localhost:8080/healthz/live >/dev/null 2>&1 && break; sleep 1; done
curl -fsS localhost:8080/healthz/live
code=$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/healthz/ready)
[ "$code" = 503 ] || { echo "readiness must fail without a database, got $code"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST localhost:8080/payments -H 'content-type: application/json' -d '{}')
[ "$code" = 422 ] || { echo "expected 422 without Idempotency-Key and body, got $code"; exit 1; }
curl -fsS localhost:8080/metrics | grep -q lab_requests_shed_total
docker exec lab-smoke id -u | grep -qx 10001 && echo "smoke OK (non-root)"
