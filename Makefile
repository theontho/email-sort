.PHONY: precheck setup-dev lint format test typecheck build smoke clean

precheck:
	uv run email-sort precheck

setup-dev:
	uv sync --all-groups
	uv run pre-commit install

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest --cov=email_sort --cov-report=term-missing

typecheck:
	uv run mypy src

build:
	uv build

smoke:
	uv run email-sort --version
	uv run email-sort precheck

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache .coverage htmlcov dist build
	find src tests -type d -name "__pycache__" -exec rm -rf {} +
