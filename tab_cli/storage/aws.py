import os
from enum import Enum

from typing import BinaryIO, Iterator
from loguru import logger

from tab_cli.storage.base import StorageBackend, FileInfo
from tab_cli.url_parser import parse_url


class AwsAuthMethod(Enum):
    EXPLICIT_KEYS = 1    # AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
    PROFILE = 2          # AWS_PROFILE or default profile (handles SSO, assume role, etc.)
    ANONYMOUS = 3        # Public buckets


class AwsBackend(StorageBackend):
    """Storage backend for AWS S3.

    URL format: s3://bucket/path

    Authentication is handled by boto3's credential chain via s3fs:
    1. Explicit keys from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
    2. Profile-based auth (AWS_PROFILE or default) - handles:
       - Shared credentials file (~/.aws/credentials)
       - AWS config file (~/.aws/config)
       - SSO credentials (from `aws sso login`)
       - Assume role
       - Container credentials (ECS/EKS)
       - EC2 instance metadata
    3. Anonymous access (for public buckets, if requested)
    """

    def __init__(self, anon: bool = False) -> None:
        """Initialize the AWS S3 storage backend.

        Args:
            anon: If True, use anonymous access (for public buckets only).
        """
        try:
            import s3fs
        except ImportError as e:
            raise ImportError("Package 's3fs' is required for s3:// URLs. Install with: pip install s3fs") from e

        self.s3fs = s3fs
        self.fs = None
        self.anon = anon

        # Get profile from environment
        self.profile = os.environ.get("AWS_PROFILE")
        self.region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

        # Check for explicit credentials in environment
        self.access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        self.secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.session_token = os.environ.get("AWS_SESSION_TOKEN")

        if anon:
            # Anonymous access for public buckets
            logger.debug("Using anonymous access for S3")
            self.fs = self.s3fs.S3FileSystem(anon=True)
            self.method = AwsAuthMethod.ANONYMOUS
            return

        # 1. Try explicit credentials from environment
        if self.access_key and self.secret_key:
            logger.debug("Authenticating to S3 using explicit credentials from environment")
            try:
                self.fs = self.s3fs.S3FileSystem(
                    key=self.access_key,
                    secret=self.secret_key,
                    token=self.session_token,
                )
                self.method = AwsAuthMethod.EXPLICIT_KEYS
                return
            except Exception as e:
                logger.debug("Explicit credentials authentication failed: {}", e)

        # 2. Fall back to profile-based auth (boto3 credential chain)
        # This handles: ~/.aws/credentials, ~/.aws/config, SSO, assume role, instance metadata
        profile_desc = f"profile '{self.profile}'" if self.profile else "default credential chain"
        logger.debug("Authenticating to S3 using {}", profile_desc)
        try:
            self.fs = self.s3fs.S3FileSystem(profile=self.profile)
            self.method = AwsAuthMethod.PROFILE
            return
        except Exception as e:
            logger.debug("Profile-based authentication failed: {}", e)

        if self.fs is None:
            raise ValueError(
                "Could not authenticate to AWS S3. "
                "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, "
                "configure ~/.aws/credentials, run 'aws configure', "
                "or run 'aws sso login'."
            )

    def normalize_for_polars(self, url: str) -> str:
        """Normalize URL to a format Polars understands.

        Polars expects s3://bucket/path format.

        Returns:
            Normalized URL in s3://bucket/path format.
        """
        parsed = parse_url(url)
        return f"s3://{parsed.bucket}/{parsed.path}"

    def storage_options(self, url: str) -> dict[str, str] | None:
        """Return storage options for Polars S3 access.

        Returns:
            Dict with appropriate authentication options for S3.
            Includes both s3fs-style keys and Rust object_store keys for compatibility.
        """
        if self.method == AwsAuthMethod.ANONYMOUS:
            return {
                "anon": True,
                "aws_skip_signature": "true",
            }

        if self.method == AwsAuthMethod.EXPLICIT_KEYS:
            opts = {
                # s3fs keys
                "key": self.access_key,
                "secret": self.secret_key,
                # object_store keys
                "aws_access_key_id": self.access_key,
                "aws_secret_access_key": self.secret_key,
            }
            if self.session_token:
                opts["token"] = self.session_token
                opts["aws_session_token"] = self.session_token
            if self.region:
                opts["client_kwargs"] = {"region_name": self.region}
                opts["aws_region"] = self.region
            return opts

        if self.method == AwsAuthMethod.PROFILE:
            # For profile-based auth, we need to fetch the resolved credentials
            # so Polars can use them (Polars doesn't understand profiles directly)
            try:
                credentials = self._get_credentials_from_session()
                if credentials:
                    opts = {
                        # s3fs keys
                        "key": credentials["access_key"],
                        "secret": credentials["secret_key"],
                        # object_store keys
                        "aws_access_key_id": credentials["access_key"],
                        "aws_secret_access_key": credentials["secret_key"],
                    }
                    if credentials.get("token"):
                        opts["token"] = credentials["token"]
                        opts["aws_session_token"] = credentials["token"]
                    if self.region:
                        opts["client_kwargs"] = {"region_name": self.region}
                        opts["aws_region"] = self.region
                    return opts
            except Exception as e:
                logger.debug("Failed to resolve credentials from session: {}", e)

            # Fallback: just pass the profile and hope Polars/fsspec can handle it
            opts = {}
            if self.profile:
                opts["profile"] = self.profile
            if self.region:
                opts["client_kwargs"] = {"region_name": self.region}
                opts["aws_region"] = self.region
            return opts if opts else None

        return None

    def _get_credentials_from_session(self) -> dict | None:
        """Get resolved credentials from boto3 session."""
        try:
            import boto3
            session = boto3.Session(profile_name=self.profile)
            credentials = session.get_credentials()
            if credentials:
                frozen = credentials.get_frozen_credentials()
                return {
                    "access_key": frozen.access_key,
                    "secret_key": frozen.secret_key,
                    "token": frozen.token,
                }
        except Exception:
            pass
        return None

    def _to_internal(self, url: str) -> str:
        """Convert URL to internal path for s3fs operations."""
        parsed = parse_url(url)
        return f"{parsed.bucket}/{parsed.path}"

    def _to_uri(self, internal_path: str) -> str:
        """Convert internal path back to s3:// URL."""
        return f"s3://{internal_path}"

    def open(self, url: str) -> BinaryIO:
        return self.fs.open(self._to_internal(url), "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        internal_path = self._to_internal(url)
        pattern = f"{internal_path}/**/*{extension}"
        for path in sorted(self.fs.glob(pattern)):
            info = self.fs.info(path)
            yield FileInfo(url=self._to_uri(path), size=info["size"])

    def size(self, url: str) -> int:
        return self.fs.size(self._to_internal(url))

    def is_directory(self, url: str) -> bool:
        path = self._to_internal(url)
        try:
            info = self.fs.info(path)
            return info.get("type") == "directory"
        except FileNotFoundError:
            try:
                contents = self.fs.ls(path, detail=False)
                return len(contents) > 0
            except Exception:
                return False
