"""Storage backends for filesystem abstraction."""

import os
from dataclasses import dataclass
from loguru import logger
from urllib.parse import urlparse

from tab_cli.storage.base import FileInfo, StorageBackend
from tab_cli.storage.local import LocalBackend
from tab_cli.url_parser import parse_url, ParsedUrl

__all__ = [
    "FileInfo",
    "StorageBackend",
    "LocalBackend",
    "get_backend",
]


def get_backend(url: str) -> StorageBackend:
    """Get the appropriate storage backend for a URL.

    Supports:
    - Local paths (no scheme or file://)
    - s3:// - AWS S3 (requires s3fs)
    - gs:// - Google Cloud Storage (requires gcsfs)
    - az:// - Azure Blob Storage (requires adlfs)
    - abfs://, abfss:// - Azure Data Lake Storage Gen2 (requires adlfs)
    - Any other fsspec-supported protocol

    For az:// URLs, the interpretation of the URL authority depends on the
    --az-url-authority-is-account global flag:
    - If set: authority is the storage account name
      - az://account/container/path
      - az:///container/path (account from AZURE_STORAGE_ACCOUNT)
    - If not set (default): authority is the container name
      - az://container/path (standard adlfs behavior)
    """
    parsed = parse_url(url)
    logger.debug(f"Accessing data from\n"
                 f" - Protocol: [bold]{parsed.scheme}[/]\n"
                 f" - Account: {parsed.account}\n"
                 f" - Bucket: {parsed.bucket}\n"
                 f" - Path: {parsed.path}"
                 )

    # Local filesystem
    if parsed.scheme == "file" or not parsed.scheme:
        return LocalBackend()

    elif parsed.scheme == "az":
        from tab_cli import config
        from tab_cli.storage.az import AzBackend

        return AzBackend(
            account=parsed.account,
            container=parsed.bucket,
            az_url_authority_is_account=config.config.az_url_authority_is_account,
        )

    elif parsed.scheme in {"abfs", "abfss"}:
        from tab_cli.storage.az import AzBackend

        # abfs/abfss always uses account in URL or env
        return AzBackend(
            account=parsed.account,
            container=parsed.bucket,
        )

    # Google Cloud Storage
    elif parsed.scheme == "gs":
        from tab_cli.storage.gcloud import GcloudBackend
        return GcloudBackend()

    # AWS S3
    elif parsed.scheme == "s3":
        from tab_cli.storage.aws import AwsBackend
        return AwsBackend()

    # All other protocols via fsspec
    from tab_cli.storage.fsspec import FsspecBackend

    return FsspecBackend(parsed.scheme)
