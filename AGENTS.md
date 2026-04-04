# AGENTS.md

## Purpose

This file gives coding agents the repository-specific commands and conventions for `tab-cli`.
Use it as the default operating guide when changing code in this repo.

## Project Snapshot

- Language: Python.
- Package manager and task runner: `uv`.
- Build backend: Hatchling.
- CLI framework: Typer.
- Data engine: Polars.
- Terminal rendering: Rich.
- Logging: Loguru.
- Main package path: `src/tab_cli`.
- Tests path: `tests`.
- Docs path: `docs` with `mkdocs.yml`.

## Setup

- Sync the development environment with `uv sync --dev`.
- Install the CLI tool locally with `uv tool install . --force`.
- Python requirement is `>=3.10`.
- Optional cloud extras exist for `s3`, `gcs`, and `azure`.
- `uv.lock` is committed; prefer `uv` commands over raw `pip` when possible.

## Makefile Notes

- `make dev` runs `uv sync --dev`.
- `make test` runs `uv run pytest`.
- `make build` runs `uv build` after `clean`.
- `make docs` runs the MkDocs build target.
- The current `Makefile` uses `tab_cli/` for `lint`, `format`, and `typecheck`.
- The actual package lives under `src/tab_cli`, so prefer direct `uv run ... src/tab_cli` commands unless the Makefile is updated.

## Test Guidance

- Prefer targeted pytest node IDs while iterating.
- Use `-q` for concise output when running a single test.
- Use `-k <expr>` when the exact node ID is inconvenient.
- The CLI tests rely on `typer.testing.CliRunner`.
- Test data is stored under `tests/assets`.
- Existing tests emphasize user-visible CLI output, not internal implementation details.
- The CLI tests are split across focused files under `tests/`; extend the nearest existing file unless a new one is clearly warranted.
- Assert both `exit_code` and key output fragments.
- For stdin support, pass `"-"` as the path and provide `input=` to `runner.invoke(...)`.

## Repository Structure

- `src/tab_cli/cli.py`: Typer entrypoint and command definitions.
- `src/tab_cli/handlers/`: reader/writer composition and CLI table output.
- `src/tab_cli/formats/`: per-format adapters for CSV, TSV, JSONL, Avro, and Parquet.
- `src/tab_cli/storage/`: local, cloud, and fsspec-backed storage backends.
- `src/tab_cli/url_parser.py`: parsing for local and cloud URLs.
- `src/tab_cli/config.py`: mutable global config for CLI flags.
- `tests/test_cli.py`: current CLI test suite.

## Architecture Conventions

- Keep the CLI layer thin.
- Put command-line option parsing in `cli.py`.
- Put reusable IO behavior in handlers, format adapters, or storage backends.
- Use `TableReader` to combine a storage backend with a format handler.
- Use `TableWriter` or `FormatWriter` for output behavior.
- Keep format-specific logic inside `src/tab_cli/formats/`.
- Keep backend-specific auth and path normalization inside `src/tab_cli/storage/`.
- Use `polars.LazyFrame` as the default data pipeline type.
- Only collect eagerly when necessary for schema inspection, batch writing, counts, or summary output.

## Imports

- Follow the repo’s existing import style: standard library, third-party, then local imports.
- Avoid unused imports; Ruff is already catching several in the repo.
- Prefer explicit imports from sibling modules over wildcard imports.
- `__init__.py` files may re-export public types via `__all__`.
- Keep imports at module scope unless a local import is needed to avoid optional dependency failures or circular imports.
- Local imports are already used when backend selection depends on optional cloud packages.

## Formatting

- Use `ruff format` for code formatting.
- Match the existing style: double quotes, compact functions, minimal vertical whitespace.
- Keep lines readable rather than aggressively compressed.
- Prefer small helper functions when a command body becomes dense.
- Do not introduce decorative comments or banner blocks.
- Keep docstrings short and factual.

## Types

- NEVER implicitly cast any variable to bool with `if var:` or `if not var:` unless the variable is already a bool. Do NOT rely on truthiness for control flow:
  for example, testing if a list is empty with `if not my_list:` is not allowed. Instead, use explicit length checks like `if len(my_list) > 0:`.
  always write `if x is not None:` or `if x is None:` when checking for `None` values.
- Type hints are used widely and should be preserved.
- Prefer modern built-in generics like `list[str]` and `dict[str, Any]`.
- Use `X | None` instead of `Optional[X]` in new code unless matching nearby style requires otherwise.
- Use `TypeAlias` and `Annotated` where CLI option reuse materially improves readability.
- Keep abstract method signatures precise in base classes.
- Return `pl.LazyFrame` for lazy operations and `pl.DataFrame` for eager stream reads.
- Be careful with optional third-party imports; they currently create type-check noise.
- When adding types around cloud storage options, prefer signatures broad enough for mixed value types if the implementation returns bools or nested dicts.

## Naming

- Use `snake_case` for functions, variables, and module names.
- Use `CamelCase` for classes and dataclasses.
- Use clear suffixes like `Format`, `Backend`, `Reader`, `Writer`, `Summary`, and `Schema`.
- For CLI option aliases in `cli.py`, the repo uses `PathArg`, `InputOpt`, `SqlOpt`, and similar names.
- Keep names explicit about data shape or role, for example `storage_options`, `table_summary`, or `parsed`.

## Error Handling

- Raise `ValueError` for invalid user input, unknown formats, unsupported schemes, or inconsistent data layout.
- Raise `ImportError` when an optional backend dependency is required but missing.
- Prefer helpful, user-actionable error messages.
- Preserve current behavior where backend auth methods fail over to the next strategy.
- Log fallback behavior with `logger.debug(...)` or `logger.warning(...)` when it aids diagnosis.
- Do not silently swallow errors unless the code is intentionally probing multiple auth mechanisms.

## Logging And Output

- The CLI configures Loguru with `RichHandler` in the Typer callback.
- When writing Loguru messages, use f-strings instead of Loguru brace-style formatting.
- User-facing table and summary output is rendered with Rich.
- Streaming command output usually writes bytes to `sys.stdout.buffer`.
- Keep stderr/stdout behavior consistent with the existing command design.
- Avoid printing directly unless a component is explicitly designed for it.

## Data And Polars Usage

- Prefer lazy scans (`pl.scan_csv`, `pl.scan_parquet`, `pl.scan_ndjson`) for file input.
- Use eager reads only for streams or when a library requires it.
- For row counts, the repo commonly uses `.select(pl.len()).collect().item()`.
- For directory reads, preserve the existing split between native glob support and manual concatenation.
- When reading directories, keep metadata and hidden-file filtering behavior intact.
- If adding a new format, implement the `FormatHandler` contract fully.

## CLI and UX Expectations

- Keep Typer command help concise.
- Reuse the existing type aliases for command arguments and options when practical.
- Maintain stdin support via `"-"` consistently across commands.
- When input comes from stdin, require explicit format selection if extension inference is impossible.
- Preserve output format inference rules unless intentionally changing UX.

## Documentation

- Update `docs/` and CLI help text when changing user-facing behavior.
- Update `CHANGELOG.md` with a clear description of user-facing changes and bug fixes.
- Keep examples aligned with the actual command names and flags.
- If you change build, test, or auth flows, reflect that in this file too.

## Agent Workflow Recommendations

- Start with targeted tests for the touched command or module.
- Run `uv run ruff check src tests` on touched files before finishing.
- Run `uv run ty check src/tab_cli` when changing signatures or backend option types.
- Run `uv run pytest` for broader validation before finalizing cross-cutting changes.
- Mention pre-existing lint or type-check failures separately from regressions you introduce.
- Update CHANGELOG.md when necessary.
- 
