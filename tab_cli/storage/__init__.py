"""Storage backends for filesystem abstraction."""

from tab_cli.storage.base import FileInfo, StorageBackend
from tab_cli.storage.local import LocalBackend

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
    - abfss:// - Azure Data Lake Storage Gen2 (requires adlfs)
    - Any other fsspec-supported protocol

    For az:// URLs, the interpretation of the URL authority depends on the
    --az-url-authority-is-account global flag:
    - If set: authority is the storage account name
      - az://account/container/path
      - az:///container/path (account from AZURE_STORAGE_ACCOUNT_NAME)
    - If not set (default): authority is the container name
      - az://container/path (standard adlfs behavior)
    """
    if "://" in url:
        protocol = url.split("://", 1)[0]
        if protocol == "file":
            return LocalBackend()

        if protocol == "az":
            from tab_cli import config
            from tab_cli.storage.az import AzBackend

            return AzBackend(az_url_authority_is_account=config.config.az_url_authority_is_account)

        from tab_cli.storage.fsspec import FsspecBackend

        return FsspecBackend(protocol)

    # Default to local filesystem
    return LocalBackend()
