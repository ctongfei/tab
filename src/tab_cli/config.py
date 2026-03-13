"""Global configuration for tab_cli-cli."""

from dataclasses import dataclass


@dataclass
class Config:
    """Global configuration settings."""

    az_url_authority_is_account: bool = False
    sampling_size_for_schema_inference: int = 32


# Global config instance
config: Config = Config()
