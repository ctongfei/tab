"""Global configuration for tab_cli-cli."""

import json
from dataclasses import dataclass, fields
from pathlib import Path
from types import UnionType
from typing import Any, Union, get_args, get_origin

from loguru import logger

CONFIG_DIR = Path.home() / ".config" / "tab"
CONFIG_FILE = CONFIG_DIR / "config.json"


@dataclass
class Config:
    """Global configuration settings."""

    az_url_authority_is_account: bool = False
    default_num_view_rows: int = 20
    log_level: str = "INFO"
    max_cell_length: int | None = None
    num_remote_workers: int = 8
    sampling_size_for_schema_inference: int = 32


# Global config instance
config: Config = Config()


def _matches_type(value: Any, expected_type: Any) -> bool:
    origin = get_origin(expected_type)
    if origin in {UnionType, Union}:
        return any(_matches_type(value, option) for option in get_args(expected_type))
    if expected_type is type(None):
        return value is None
    return type(value) is expected_type


def load_config_file(path: Path = CONFIG_FILE) -> None:
    """Load settings from a JSON config file into the global config.

    Unknown keys are logged and ignored. Type mismatches raise ValueError.
    If the file does not exist, this is a no-op.
    """
    if not path.is_file():
        logger.debug(f"No config file found at {path}; using built-in defaults")
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
            logger.warning(f"Unknown config key '{key}' in {path}")
            continue
        expected_type = known[key]
        expected_name = getattr(expected_type, "__name__", str(expected_type))
        if _matches_type(value, expected_type) is False:
            raise ValueError(
                f"Config key '{key}' must be of type {expected_name}, got {type(value).__name__}"
            )
        setattr(config, key, value)
    logger.debug(f"Loaded config from {path}")
