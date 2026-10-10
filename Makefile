.PHONY: up down logs verify lint test fetch-data build-data verify-data train-model backtest live-ingest verify-live

# LightGBM's macOS wheel needs the libomp OpenMP runtime, which it expects
# at a Homebrew path this project deliberately doesn't use. scikit-learn's
# wheel already bundles a compatible libomp.dylib -- point the dynamic
# linker at it instead of installing anything new. See DECISIONS.md.
LIBOMP_DIR := $(shell .venv/bin/python -c "import sklearn, os; print(os.path.join(os.path.dirname(sklearn.__file__), '.dylibs'))")

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
