from dataclasses import dataclass
import os
from urllib.parse import urlparse


@dataclass
class ParsedUrl:
    """Parsed URL components for cloud storage."""

    scheme: str
    """URL scheme (e.g., 's3', 'gs', 'az', 'abfs', 'abfss', 'file', or '' for local)."""

    bucket: str | None
    """Bucket name for s3:// and gs://, container for az://, abfs://, and abfss://."""

    account: str | None
    """Storage account name for Azure (az://, abfs://, abfss://)."""

    path: str
    """Path within the bucket/container, or full local path."""

    original: str
    """Original URL string."""


def parse_url(url: str) -> ParsedUrl:
    """Parse a storage URL into its components.

    Supports:
    - Local paths: /path/to/file, ./relative/path, file:///path/to/file
    - S3: s3://bucket/path
    - GCS: gs://bucket/path
    - Azure Blob (az://):
      - With --az-url-authority-is-account: az://account/container/path
      - Without (default): az://container/path (account from AZURE_STORAGE_ACCOUNT)
    - Azure Data Lake (abfs://, abfss://):
      - abfs[s]://container@account.dfs.core.windows.net/path
      - abfs[s]://container/path (account from AZURE_STORAGE_ACCOUNT)

    Returns:
        ParsedUrl with scheme, bucket, account, and path components.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower() if parsed.scheme else ""

    # Local file paths
    if not scheme or scheme == "file":
        # file:///path or file://localhost/path -> /path
        # No scheme -> treat as local path
        if scheme == "file":
            path = parsed.path
        else:
            path = url
        return ParsedUrl(scheme="file", bucket=None, account=None, path=path, original=url)

    # S3/GCS: s3://bucket/path
    if scheme in {"s3", "gs"}:
        bucket = parsed.netloc
        path = parsed.path.lstrip("/")
        return ParsedUrl(scheme=scheme, bucket=bucket, account=None, path=path, original=url)

    # Azure Blob: az://container/path or az://account/container/path
    if scheme == "az":
        from tab_cli import config

        if config.config.az_url_authority_is_account:
            # az://account/container/path or az:///container/path
            account = parsed.netloc if parsed.netloc else os.environ.get("AZURE_STORAGE_ACCOUNT")
            # Path is /container/path, first segment is container
            path_parts = parsed.path.lstrip("/").split("/", 1)
            container = path_parts[0] if path_parts else None
            path = path_parts[1] if len(path_parts) > 1 else ""
        else:
            # az://container/path (default adlfs behavior)
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
            container = parsed.netloc
            path = parsed.path.lstrip("/")
        return ParsedUrl(scheme=scheme, bucket=container, account=account, path=path, original=url)

    # Azure Data Lake: abfs[s]://container@account.dfs.core.windows.net/path
    # or abfs[s]://container/path
    if scheme in ("abfs", "abfss"):
        netloc = parsed.netloc
        if "@" in netloc:
            # container@account.dfs.core.windows.net
            container, host = netloc.split("@", 1)
            # Extract account from host (account.dfs.core.windows.net)
            account = host.split(".")[0] if "." in host else host
        else:
            # container only, account from env
            container = netloc
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
        path = parsed.path.lstrip("/")
        return ParsedUrl(scheme=scheme, bucket=container, account=account, path=path, original=url)

    # Other schemes: return with netloc as bucket-like component
    raise ValueError(f"Unsupported scheme: {scheme}")
