SHELL := /bin/bash

.PHONY: install dev clean lint format test build mkdocs-build mkdocs-serve docs publish publish-test gh-deploy-docs

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

mkdocs-build:
	sh ./docs/gen_assets.sh
	mkdocs build --strict

mkdocs-serve:
	mkdocs serve --dev-addr=127.0.0.1:8000

docs: mkdocs-build

publish: build
	uv publish

publish-test: build
	uv publish --publish-url https://test.pypi.org/legacy/

gh-deploy-docs: docs
	set -ex ; \
	WORK="$$( mktemp -d )" ; \
	VER="$$( git describe --always --tags --dirty )" ; \
	git worktree add --force "$$WORK" gh-pages ; \
	rm -rf "$$WORK"/* ; \
	rsync -av site/ "$$WORK"/ ; \
	if [ -f CNAME ] ; then cp CNAME "$$WORK"/ ; fi ; \
	pushd "$$WORK" ; \
	git add -A ; \
	git commit -m "Updated gh-pages $$VER" ; \
	popd ; \
	git worktree remove "$$WORK" ; \
	git push origin gh-pages
