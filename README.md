# tab

A CLI tool for viewing, querying, and converting tabular data files.

## Supported Formats
 - Parquet
 - Avro
 - CSV
 - TSV
 - Jsonl

## Usage

### View data

Display rows from a tabular data file:

```bash
tab view data.parquet
tab view data.csv --limit 20
tab view data.tsv --skip 100 --limit 50
```

Output to different formats:

```bash
tab view data.parquet -o jsonl
tab view data.parquet -o csv
```

### Schema

Display the schema (column names and types):

```bash
tab schema data.parquet
```

### Summary

Display summary information about a file:

```bash
tab summary data.parquet
```

### SQL queries

Run SQL queries on your data. The table is referenced as `t`:

```bash
tab sql "SELECT * FROM t WHERE age > 30" data.parquet
tab sql "SELECT name, COUNT(*) FROM t GROUP BY name" data.csv
```

### Convert

Convert between formats:

```bash
tab convert data.csv data.parquet
tab convert data.parquet data.jsonl -o jsonl
```

Write partitioned output:

```bash
tab convert data.csv output_dir/ -o parquet -n 4
```

## Options

### Common options

| Option    | Description                                                                   |
|-----------|-------------------------------------------------------------------------------|
| `-i`      | Input format (`parquet`, `csv`, `tsv`, `jsonl`). Auto-detected from extension. |
| `-o`      | Output format (`parquet`, `csv`, `tsv`, `jsonl`).                             |
| `--limit` | Maximum number of rows to display.                                            |
| `--skip`  | Number of rows to skip from the beginning.                                    |

### Convert options

| Option | Description |
|--------|-------------|
| `-n`   | Number of output partitions. Creates a directory with part files. |

