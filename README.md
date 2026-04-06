# tab

[![pypi](https://img.shields.io/pypi/v/tab-cli)](https://pypi.org/project/tab-cli/)

A CLI tool for viewing, querying, and converting tabular data files.
Reads CSV, TSV, JSON Lines, Parquet, Avro, SQLite tables, and DuckDB tables -- locally or from S3, GCS, and Azure Blob Storage.

```sh
pip install tab-cli
```

**Documentation**: [tongfei.me/tab](https://tongfei.me/tab)

---

## Quick look

### View any tabular file

```bash
tab view data.csv
```

<p align="center">
  <img src="https://raw.githubusercontent.com/ctongfei/tab/refs/heads/gh-pages/assets/test.svg" alt="tab view" width="680">
</p>

### Can be remote, with many partitions that is globbed together:

```bash
tab view 's3://aws-public-blockchain/v1.0/btc/blocks/date=2026-01-*/*'
````
### Query with SQL

The table is always available as `t`:

```bash
tab view --sql 'SELECT * FROM t WHERE Metric_A_Value > 80' data.csv
```

<p align="center">
  <img src="https://raw.githubusercontent.com/ctongfei/tab/refs/heads/gh-pages/assets/test-where.svg" alt="tab view --sql" width="680">
</p>

### Reshape rows with JMESPath

```bash
tab view --jp '{id: participant.id, city: profile.address.city}' data.parquet 
```

### Convert between formats

```bash
tab convert data.csv data.parquet
tab convert data.parquet data.jsonl -o jsonl
tab convert data.csv output_dir/ -o parquet -n 4   # partitioned
```

### Concatenate files

```bash
tab cat part1.csv part2.csv part3.csv -o jsonl > combined.jsonl
```

### Inspect schema and summary

```bash
tab schema data.parquet
tab summary data.parquet
```

### Read from stdin

```bash
curl -s https://example.com/data.csv | tab view -i csv -
```

### Read from cloud storage

```bash
tab view s3://bucket/path/data.parquet
tab view gs://bucket/path/data.csv
tab view az://container/path/data.jsonl
```

Globbing is supported for local and cloud paths:

```bash
tab view 'data/date=*/*.parquet'
tab view 's3://bucket/path/date=2026-01-*/*'
```

### Read a SQLite table

Use `{url}#{table_name}` for SQLite inputs:

```bash
tab view data.db#users
tab view s3://bucket/path/data.db#users
```

### Read a DuckDB table

Use `{url}#{table_name}` for DuckDB inputs:

```bash
tab view data.duckdb#users
tab view s3://bucket/path/data.duckdb#users
```

Install cloud extras as needed:

```sh
pip install 'tab-cli[s3]'    # AWS S3
pip install 'tab-cli[gs]'    # Google Cloud Storage
pip install 'tab-cli[az]'    # Azure Blob Storage
pip install 'tab-cli[duckdb]'  # DuckDB input support
pip install 'tab-cli[sqlite]'  # SQLite via Polars ADBC
pip install 'tab-cli[all]'   # Install all optional integrations
```

## Supported formats
 - csv
 - tsv
 - jsonl
 - parquet
 - avro
 - duckdb (input only; use `{url}#{table_name}`)
 - sqlite (input only; use `{url}#{table_name}`)
