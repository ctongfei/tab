# Configuration

`tab` supports a global configuration file at `~/.config/tab/config.json`. Settings in this file serve as defaults and are overridden by CLI flags.

## Setup

Create the config file:

```bash
mkdir -p ~/.config/tab
cat > ~/.config/tab/config.json << 'EOF'
{
  "az_url_authority_is_account": false,
  "default_num_view_rows": 20,
  "log_level": "INFO",
  "max_cell_length": null,
  "num_remote_workers": 8,
  "sampling_size_for_schema_inference": 32
}
EOF
```

## Available settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `az_url_authority_is_account` | `bool` | `false` | Interpret `az://` URL authority as storage account name instead of container name. |
| `default_num_view_rows` | `int` | `20` | Default number of rows shown by `tab view` when `--limit` is omitted. |
| `log_level` | `str` | `"INFO"` | Default CLI log level when `--log-level` is omitted. |
| `max_cell_length` | `int \| null` | `null` | Default maximum cell length for `tab view`. The CLI `--max-cell-length` flag overrides it. |
| `num_remote_workers` | `int` | `8` | Maximum worker threads for remote per-partition summary work such as Parquet row counts. |
| `sampling_size_for_schema_inference` | `int` | `32` | Number of rows sampled for schema inference (e.g. when using `--jp`). |

## Precedence

Settings are applied in this order (last wins):

1. Built-in defaults
2. Config file (`~/.config/tab/config.json`)
3. CLI flags (e.g. `--az-url-authority-is-account`)

If the config file does not exist, built-in defaults are used. Unknown keys in the file are ignored with a warning.
