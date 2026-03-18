PYTHON ?= python3

.PHONY: install format lint test typecheck run-api migrate list-migrations

install:
	$(PYTHON) -m pip install -e ".[dev]"

format:
	$(PYTHON) -m black app tests infra/scripts

lint:
	$(PYTHON) -m ruff check app tests infra/scripts

test:
	$(PYTHON) -m pytest

typecheck:
	$(PYTHON) -m mypy app infra/scripts

run-api:
	$(PYTHON) -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

migrate:
	$(PYTHON) -m infra.scripts.run_migrations

list-migrations:
	$(PYTHON) -m infra.scripts.run_migrations --list
