# CLI Reference

## `tab view`

View tabular data from a data file, or a directory of partitions of data files.

```bash
tab view $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). Auto-detected from extension if omitted.         |
| `-o` / `--output-format` | Output format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). If not specified, print Rich table in terminal. |
| `--limit`               | Maximum number of rows to display.                                                                        |
| `--skip`                | Number of rows to skip from the beginning.                                                                |

## `tab schema`

Display the schema of a tabular data file.

```bash
tab schema $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). Auto-detected from extension if omitted.         |


## `tab summary`

Display summary information about a tabular data file.

```bash
tab summary $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). Auto-detected from extension if omitted.         |


## `tab sql`

Run a SQL query on tabular data. The table is available as `t`.

```bash
tab sql $query $path [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). Auto-detected from extension if omitted.         |
| `-o` / `--output-format` | Output format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). If not specified, print Rich table in terminal. |
| `--limit`               | Maximum number of rows to display.                                                                        |
| `--skip`                | Number of rows to skip from the beginning.                                                                |

## `tab convert`

Convert tabular data from one format to another.

```bash
tab convert $src $dst [OPTIONS]
```

Options:

| Option                  | Description                                                                                             |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). Auto-detected from extension if omitted.       |
| `-o` / `--output-format` | Output format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). If not specified, inherits from input format. |
| `-n` / `--num-partitions` | Number of output partitions. Creates a directory with partition files.                                  |


## `tab cat`

Concatenate tabular data from multiple files.

```bash
tab cat $paths [OPTIONS]
```

Options:

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `-i` / `--input-format` | Input format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). Auto-detected from extension if omitted.         |
| `-o` / `--output-format` | Output format (`parquet`, `csv`, `tsv`, `jsonl`, `avro`). If not specified, print Rich table in terminal. |


## Global options

| Option                  | Description                                                                                                                  |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `--az-url-authority-is-account` | Interpret az:// URL authority as storage account name instead of container name. See [azure.md](Azure) for more information. |
| `--log-level`               | Log level from `{DEBUG, INFO, WARNING, ERROR, CRITICAL}`.                                                                     |
