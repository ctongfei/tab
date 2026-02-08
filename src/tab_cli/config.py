"""Global configuration for tab_cli-cli."""

from dataclasses import dataclass


@dataclass
class Config:
    """Global configuration settings."""

    az_url_authority_is_account: bool = False


# Global config instance
config: Config = Config()
