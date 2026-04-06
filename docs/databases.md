# Databases

`tab` can read SQLite and DuckDB tables as tabular input.

## Install

Database support is optional:

```bash
pip install 'tab-cli[sqlite]'
pip install 'tab-cli[duckdb]'
pip install 'tab-cli[all]'
```

If a dependency is missing, `tab` will raise an error telling you which extra to install.

## Input Syntax

SQLite and DuckDB input use this form:

```text
{url}#{table_name}
```

The `#table_name` suffix is required.

## SQLite

Examples:

```bash
tab view data.db#users
tab schema data.sqlite#events
tab summary data.sqlite3#measurements
```

SQLite file extension inference supports:

- `.db`
- `.sqlite`
- `.sqlite3`

## DuckDB

Examples:

```bash
tab view data.duckdb#users
tab schema warehouse.duckdb#events
tab summary analytics.ddb#measurements
```

DuckDB file extension inference supports:

- `.duckdb`
- `.ddb`

## Local And Remote Files

Both SQLite and DuckDB input work with any storage backend supported by `tab`.

Examples:

```bash
tab view /tmp/data.db#users
tab view s3://bucket/path/data.db#users
tab view gs://bucket/path/data.sqlite#users
tab view az://container/path/data.sqlite3#users

tab view /tmp/data.duckdb#users
tab view s3://bucket/path/data.duckdb#users
tab view gs://bucket/path/data.ddb#users
tab view az://container/path/data.duckdb#users
```

For local files, `tab` queries the database directly.

For remote files, `tab` downloads the database file to a temporary local file and then queries it with Polars.

## Supported Commands

SQLite and DuckDB are supported as input for:

```bash
tab view data.db#users
tab schema data.db#users
tab summary data.db#users
tab cat data.db#users -o csv
tab convert data.db#users output.jsonl -o jsonl

tab view data.duckdb#users
tab schema data.duckdb#users
tab summary data.duckdb#users
tab cat data.duckdb#users -o csv
tab convert data.duckdb#users output.jsonl -o jsonl
```

## Output Behavior

SQLite and DuckDB are input-only.

`tab` does not write SQLite or DuckDB output, so commands that need a streamed output format must use `-o` explicitly:

```bash
tab cat data.db#users -o csv
tab cat data.db#users -o jsonl

tab cat data.duckdb#users -o csv
tab cat data.duckdb#users -o jsonl
```

This is required because SQLite and DuckDB are database containers, not streamable stdout formats.

## Notes

- Database input must reference a single file, not a directory or glob.
- The table name is treated as a table identifier, not a free-form SQL query.
