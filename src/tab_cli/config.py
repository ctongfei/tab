"""Global configuration for tab_cli-cli."""

import json
from dataclasses import dataclass, fields
from pathlib import Path

from loguru import logger

CONFIG_DIR = Path.home() / ".config" / "tab"
CONFIG_FILE = CONFIG_DIR / "config.json"


@dataclass
class Config:
    """Global configuration settings."""

    az_url_authority_is_account: bool = False
    sampling_size_for_schema_inference: int = 32


# Global config instance
config: Config = Config()


def load_config_file(path: Path = CONFIG_FILE) -> None:
    """Load settings from a JSON config file into the global config.

    Unknown keys are logged and ignored. Type mismatches raise ValueError.
    If the file does not exist, this is a no-op.
    """
    if not path.is_file():
        return

    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(
            f"Config file must contain a JSON object, got {type(data).__name__}"
        )

    known = {f.name: f.type for f in fields(Config)}
    for key, value in data.items():
        if key not in known:
            logger.warning("Unknown config key '{}' in {}", key, path)
            continue
        setattr(config, key, value)
    logger.debug("Loaded config from {}", path)
