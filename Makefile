.PHONY: install install-dev validate format lint typecheck security test

install:
	python -m pip install -r requirements.txt

install-dev:
	python -m pip install -r requirements-dev.txt

validate:
	python scripts/validate_environment.py

format:
	python -m ruff format src tests scripts

lint:
	python -m ruff check src tests scripts

typecheck:
	python -m mypy src

security:
	python -m bandit -c pyproject.toml -r src

test:
	python -m pytest --cov=retail_lakehouse --cov-report=term-missing
