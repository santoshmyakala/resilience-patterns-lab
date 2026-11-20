# syntax=docker/dockerfile:1
# One image, three entry points: API (default), worker (python -m lab.worker), migration (python -m lab.db).
FROM python:3.15-slim AS build
WORKDIR /src
COPY pyproject.toml ./
COPY lab ./lab
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.15-slim
LABEL org.opencontainers.image.source="https://github.com/santoshmyakala/resilience-patterns-lab" \
      org.opencontainers.image.description="Resilience patterns lab: payments API and idempotent worker" \
      org.opencontainers.image.licenses="MIT"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
# Apply Debian security updates so fixed CVEs in the base image don't ship (enforced by Trivy in CI).
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 app
COPY --from=build /install /usr/local
USER 10001
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8080/healthz/live'); sys.exit(0)"
CMD ["uvicorn", "--factory", "lab.api:main", "--host", "0.0.0.0", "--port", "8080"]
