.PHONY: up down logs verify lint test fetch-data build-data verify-data train-model backtest live-ingest verify-live rotation-produce rotation-job rotation-verify sync-features predict-service verify-predict trip-schema trip-worker trip-start verify-trip-monitoring web-api web

# LightGBM's macOS wheel needs the libomp OpenMP runtime, which it expects
# at a Homebrew path this project deliberately doesn't use. scikit-learn's
# wheel already bundles a compatible libomp.dylib -- point the dynamic
# linker at it instead of installing anything new. See DECISIONS.md.
LIBOMP_DIR := $(shell .venv/bin/python -c "import sklearn, os; print(os.path.join(os.path.dirname(sklearn.__file__), '.dylibs'))")

# PyFlink needs a real JVM (Java 17+) -- downloaded user-locally (Temurin,
# no Homebrew) to ~/.local/jdk. Its Python UDF runner also spawns a `python`
# subprocess, which needs .venv/bin on PATH (not just invoked directly) to
# find it. See DECISIONS.md.
JAVA_HOME := $(HOME)/.local/jdk/jdk-17.0.20.1+1/Contents/Home
FLINK_PATH := $(PWD)/.venv/bin:$(PATH)

up:
	docker compose -f infra/docker-compose.yml up -d --wait --wait-timeout 180

down:
	docker compose -f infra/docker-compose.yml down -v

logs:
	docker compose -f infra/docker-compose.yml logs -f

verify:
	.venv/bin/python scripts/verify_stack.py

lint:
	.venv/bin/ruff check .

test:
	.venv/bin/pytest -q

fetch-data:
	.venv/bin/python scripts/fetch_historical_data.py

build-data:
	.venv/bin/python scripts/build_historical_data.py

verify-data:
	.venv/bin/python scripts/verify_historical_data.py

train-model:
	DYLD_LIBRARY_PATH="$(LIBOMP_DIR)" .venv/bin/python scripts/train_model.py

backtest:
	DYLD_LIBRARY_PATH="$(LIBOMP_DIR)" .venv/bin/python scripts/run_backtest.py

live-ingest:
	.venv/bin/python scripts/run_live_ingestion.py

verify-live:
	.venv/bin/python scripts/verify_live_ingestion.py

rotation-produce:
	.venv/bin/python scripts/produce_flight_events.py

rotation-job:
	JAVA_HOME="$(JAVA_HOME)" PATH="$(FLINK_PATH)" .venv/bin/python -m layover_stream.rotation_job --bounded

rotation-verify:
	.venv/bin/python scripts/verify_rotation_job.py

sync-features:
	.venv/bin/python scripts/sync_features_to_redis.py --duration 20

predict-service:
	DYLD_LIBRARY_PATH="$(LIBOMP_DIR)" .venv/bin/python -m uvicorn layover_predict.service:app --app-dir src --port 8000

verify-predict:
	DYLD_LIBRARY_PATH="$(LIBOMP_DIR)" .venv/bin/python scripts/verify_prediction_service.py

trip-schema:
	.venv/bin/python -c "import sys; sys.path.insert(0, 'src'); from layover_trip.db import connect, apply_schema; apply_schema(connect())"

trip-worker:
	.venv/bin/python scripts/run_trip_worker.py

trip-start:
	.venv/bin/python scripts/start_trip_monitor.py

verify-trip-monitoring:
	.venv/bin/python scripts/verify_trip_monitoring.py

web-api:
	.venv/bin/python -m uvicorn layover_web.api:app --app-dir src --port 8001

web:
	cd web && npm run dev -- --port 3001
