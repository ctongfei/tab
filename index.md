# tab

A CLI tool for viewing, querying, and converting tabular data files. Supports AWS / Azure / Google Cloud Storage URLs.

## Supported Formats
 - Jsonl
 - CSV
 - TSV
 - Parquet
 - Avro

## Usage

### View data

Display rows from a tabular data file:

```bash
tab view data.csv
```
![tab view](assets/test.svg)

Output to different formats:

```bash
tab view data.parquet -o jsonl
tab view data.parquet -o csv
```

### Show schema

```bash
tab schema data.parquet
```

### Show summary

```bash
tab summary data.parquet
```

### SQL queries

Run SQL queries on your data. The table is referenced as `t`:

```bash
tab sql 'SELECT * FROM t WHERE Metric_A_Value > 80' test.csv
```
![tab sql](assets/test-where.svg)

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

### Concatenate multiple files

```bash
tab cat data1.csv data2.csv data3.csv -o jsonl > output.jsonl
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

