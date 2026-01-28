.PHONY: install dev clean lint format test build publish publish-test

install:
	uv tool install . --force

dev:
	uv sync --dev

clean:
	rm -rf dist/ build/ *.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

lint:
	uv run ruff check tab_cli/

format:
	uv run ruff format tab_cli/

typecheck:
	uv run ty check tab_cli/

test:
	uv run pytest

build: clean
	uv build

publish: build
	uv publish

publish-test: build
	uv publish --publish-url https://test.pypi.org/legacy/
