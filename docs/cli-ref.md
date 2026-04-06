# CLI Reference

## `tab view`

View tabular data from a data file in a rich CLI format, or a directory of partitions of data files.

Paths may also use glob patterns such as `data/date=*/*.parquet` or `s3://bucket/path/date=2026-01-*/*`.

```bash
tab view $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`, `sqlite`, `duckdb`). Auto-detected from extension if omitted. SQLite and DuckDB input use `{url}#{table_name}`. |
| `--sql`                 | SQL query to apply before displaying. The table is available as `t`. Mutually exclusive with `--jmespath`. |
| `--jmespath` / `--jp`   | JMESPath expression to apply to each row as JSON. Object outputs become columns; non-object outputs go to a `value` column. The result shape must stay consistent across rows. |
| `--limit`               | Maximum number of rows to display.                                                                        |
| `--skip`                | Number of rows to skip from the beginning.                                                                |
| `--max-cell-length`    | Truncate cell contents longer than this. If omitted, `max_cell_length` from config is used when set.     |

## `tab schema`

Display the schema of a tabular data file.

Paths may also use glob patterns.

```bash
tab schema $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`, `sqlite`, `duckdb`). Auto-detected from extension if omitted. SQLite and DuckDB input use `{url}#{table_name}`. |


## `tab summary`

Display summary information about a tabular data file.

Paths may also use glob patterns.

```bash
tab summary $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`, `sqlite`, `duckdb`). Auto-detected from extension if omitted. SQLite and DuckDB input use `{url}#{table_name}`. |


## `tab convert`

Convert tabular data from one format to another.

Input paths may also use glob patterns when the input format supports multi-file reads.

```bash
tab convert $src $dst [OPTIONS]
```

Options:

| Option                  | Description                                                                                             |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`, `sqlite`, `duckdb`). Auto-detected from extension if omitted. SQLite and DuckDB input use `{url}#{table_name}`. |
| `-o` / `--output-format` | Output format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). If not specified, inherits from input format. |
| `--sql`                 | SQL query to apply before writing. The table is available as `t`. Mutually exclusive with `--jmespath`. |
| `--jmespath` / `--jp`   | JMESPath expression to apply to each row as JSON. Object outputs become columns; non-object outputs go to a `value` column. The result shape must stay consistent across rows. |
| `-n` / `--num-partitions` | Number of output partitions. Creates a directory with partition files.                                  |


## `tab cat`

Concatenate tabular data from multiple files.

Input paths may also use glob patterns.

```bash
tab cat $paths [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`, `sqlite`, `duckdb`). Auto-detected from extension if omitted. SQLite and DuckDB input use `{url}#{table_name}`. |
| `-o` / `--output-format` | Output format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). If not specified, print Rich table in terminal. |
| `--sql`                 | SQL query to apply after concatenation. The table is available as `t`. Mutually exclusive with `--jmespath`. |
| `--jmespath` / `--jp`   | JMESPath expression to apply to each row as JSON. Object outputs become columns; non-object outputs go to a `value` column. The result shape must stay consistent across rows. |


## Global options

| Option                  | Description                                                                                                                  |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `--az-url-authority-is-account` | Interpret az:// URL authority as storage account name instead of container name. See [Cloud](cloud.md) for more information. |
| `--log-level`               | Log level from `{DEBUG, INFO, WARNING, ERROR, CRITICAL}`. If omitted, uses `log_level` from config.                          |
