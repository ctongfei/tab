# Configuration

`tab` supports a global configuration file at `~/.config/tab/config.json`. Settings in this file serve as defaults and are overridden by CLI flags.

## Setup

Create the config file:

```bash
mkdir -p ~/.config/tab
cat > ~/.config/tab/config.json << 'EOF'
{
  "az_url_authority_is_account": false,
  "sampling_size_for_schema_inference": 32
}
EOF
```

## Available settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `az_url_authority_is_account` | `bool` | `false` | Interpret `az://` URL authority as storage account name instead of container name. |
| `sampling_size_for_schema_inference` | `int` | `32` | Number of rows sampled for schema inference (e.g. when using `--jp`). |

## Precedence

Settings are applied in this order (last wins):

1. Built-in defaults
2. Config file (`~/.config/tab/config.json`)
3. CLI flags (e.g. `--az-url-authority-is-account`)

If the config file does not exist, built-in defaults are used. Unknown keys in the file are ignored with a warning.
