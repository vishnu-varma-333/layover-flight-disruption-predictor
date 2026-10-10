.PHONY: up down logs verify lint test fetch-data build-data verify-data

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
