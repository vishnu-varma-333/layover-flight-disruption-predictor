# Milestone 10: one shared image for every long-running Python service
# (prediction service, internal web API, Temporal trip worker) plus the
# one-off data/training/backtest scripts -- they all install the same
# `layover` package from the same pyproject.toml, so one image with a
# different `command:` per docker-compose service (see
# infra/docker-compose.prod.yml) is simpler than three near-identical
# Dockerfiles, at the cost of each service's image carrying dependencies
# (e.g. apache-flink) it doesn't itself use -- an acceptable trade for
# the spec's own "one modest VM for the demo" cost guidance, not the
# right call at a scale where per-service image size actually matters.
# See DECISIONS.md.
FROM python:3.9-slim AS base

# libgomp1: LightGBM's OpenMP runtime -- unlike macOS (see DECISIONS.md's
# libomp story), Debian's standard package covers this with no special
# linking tricks.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e .

# Trained models and processed historical data are gitignored (they're
# build/training output, not source) and not pulled from S3 here, since
# this image is built and verified locally, never pushed or run against
# real AWS in this environment (see Milestone 10's AWS-deploy decision
# in DECISIONS.md). A real deploy would instead have the container pull
# the active MLflow model and flights.parquet from the S3 bucket
# infra/terraform provisions at startup -- noted there, not built here.
COPY mlruns ./mlruns
COPY data/processed ./data/processed
COPY data/raw/reference ./data/raw/reference
COPY scripts ./scripts

ENV PYTHONUNBUFFERED=1

EXPOSE 8000 8001
