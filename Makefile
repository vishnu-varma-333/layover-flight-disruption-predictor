.PHONY: up down logs verify lint test

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
