import os
from enum import Enum

from typing import Any, BinaryIO, Iterator
from loguru import logger

from tab_cli.storage import StorageBackend, FileInfo
from tab_cli.url_parser import parse_url


class AzAuthMethod(Enum):
    CONNECTION_STRING = 1
    ACCOUNT_KEY = 2
    SAS_TOKEN = 3
    AZURE_AD = 4
    AZURE_CLI = 5


class AzBackend(StorageBackend):
    """Storage backend for Azure Blob Storage with configurable URL interpretation.

    When az_url_authority_is_account=True:
        - az://account/container/path - authority is the storage account name
        - az:///container/path - account inferred from AZURE_STORAGE_ACCOUNT

    When az_url_authority_is_account=False (default adlfs behavior):
        - az://container/path - authority is the container name
    """

    def __init__(self, account: str | None, container: str | None, az_url_authority_is_account: bool = False) -> None:
        """Initialize the Azure Blob Storage backend.

        Args:
            az_url_authority_is_account: If True, interpret the URL authority as the
                storage account name. If False, interpret it as the container name.
        """
        try:
            import adlfs
        except ImportError as e:
            raise ImportError("Package 'adlfs' is required for az:// URLs. Install with: pip install adlfs") from e

        self.adlfs = adlfs
        self.fs = None
        self.url_authority_is_account = az_url_authority_is_account

        if account is None:
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        account_key = os.environ.get("AZURE_STORAGE_KEY")
        sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")

        self.account = account
        if account is None:
            raise ValueError("Storage account name must be specified. Use az://account/container/path and use `tab --az-url-authority-is-account`, or set the environment variable.")

        # 1. Try connection string from environment
        if connection_string:
            logger.debug("Authenticating to Azure storage account '{}' using connection string", account)
            self.fs = self.adlfs.AzureBlobFileSystem(connection_string=connection_string)
            self.connection_string = connection_string
            self.method = AzAuthMethod.CONNECTION_STRING

        # 2. Try account key from environment
        elif account_key:
            logger.debug("Authenticating to Azure storage account '{}' using account key", account)
            self.fs = self.adlfs.AzureBlobFileSystem(
                account_name=account,
                account_key=account_key,
            )
            self.account_key = account_key
            self.method = AzAuthMethod.ACCOUNT_KEY

        # 3. Try SAS token from environment
        elif sas_token:
            logger.debug("Authenticating to Azure storage account '{}' using SAS token", account)
            self.fs = self.adlfs.AzureBlobFileSystem(
                account_name=account,
                sas_token=sas_token,
            )
            self.sas_token = sas_token
            self.method = AzAuthMethod.SAS_TOKEN

        else:
            # 4. Try Azure AD / RBAC (DefaultAzureCredential)
            try:
                from azure.identity.aio import DefaultAzureCredential  # Async version,

                logger.debug("Authenticating to Azure storage account '{}' using Azure AD / RBAC", account)
                self.fs = self.adlfs.AzureBlobFileSystem(
                    account_name=account,
                    credential=DefaultAzureCredential(),
                )
                self.fs.ls(container)
                self.method = AzAuthMethod.AZURE_AD

            except ImportError:
                logger.debug("azure-identity not installed, skipping Azure AD / RBAC authentication")
            except Exception as e:
                logger.debug(f"Azure AD / RBAC authentication failed: {e}")

        if self.fs is None:
            # 5. Fallback to fetching account key via Azure CLI
            logger.debug(f"Attempting to fetch account key via Azure CLI for '{account}'")
            try:
                account_key = self._get_account_key_via_cli(account)
            except Exception as e:
                logger.debug(f"Fetching account key via Azure CLI failed: {e}")
            try:
                if account_key:
                    logger.debug(f"Authenticating to Azure storage account '{account}' using key from Azure CLI")
                    self.fs = self.adlfs.AzureBlobFileSystem(
                        account_name=account,
                        account_key=account_key,
                    )
                    self.account_key = account_key
                    self.fs.ls(container)
                    self.method = AzAuthMethod.AZURE_CLI
            except Exception as e:
                logger.debug(f"Authenticating to Azure storage account '{account}' using key from Azure CLI failed: {e}")

        if self.fs is None:
            raise ValueError(
                f"Could not authenticate to storage account '{account}'. "
                "Set AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_KEY, AZURE_STORAGE_SAS_TOKEN, "
                "configure Azure AD RBAC, or run 'az login'."
            )

    def _get_account_key_via_cli(self, account: str) -> str | None:
        """Try to get storage account key via Azure CLI."""
        import subprocess

        try:
            result = subprocess.run(
                ["az", "storage", "account", "keys", "list",
                 "--account-name", account,
                 "--query", "[0].value",
                 "-o", "tsv"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None

    def normalize_for_polars(self, url: str) -> str:
        """Normalize URL to a format Polars understands.

        Polars expects az://container/path format.

        Returns:
            Normalized URL in az://container/path format.
        """
        parsed = parse_url(url)
        return f"az://{parsed.bucket}/{parsed.path}"

    def storage_options(self, url: str) -> dict[str, str] | None:
        """Return storage options for Polars Azure access.

        Returns:
            Dict with appropriate authentication options for Azure.
            Includes both adlfs-style keys and Rust object_store keys for compatibility.
            Priority: connection_string > account_key > sas_token > CLI key > account_name only
        """
        if self.method == AzAuthMethod.CONNECTION_STRING:
            return {
                "connection_string": self.connection_string,
                "azure_storage_connection_string": self.connection_string,
            }
        elif self.method == AzAuthMethod.ACCOUNT_KEY:
            return {
                "account_name": self.account,
                "account_key": self.account_key,
                "azure_storage_account_name": self.account,
                "azure_storage_account_key": self.account_key,
            }
        # 3. SAS token from environment
        elif self.method == AzAuthMethod.SAS_TOKEN:
            return {
                "account_name": self.account,
                "sas_token": self.sas_token,
                "azure_storage_account_name": self.account,
                "azure_storage_sas_token": self.sas_token,
            }
        # 4. Try fetching account key via Azure CLI
        elif self.method == AzAuthMethod.AZURE_CLI:
            return {
                "account_name": self.account,
                "account_key": self.account_key,
                "azure_storage_account_name": self.account,
                "azure_storage_account_key": self.account_key,
            }
        # Fallback: account name only (will use DefaultAzureCredential)
        else:
            return {
                "account_name": self.account,
                "azure_storage_account_name": self.account,
                "anon": 'false',
                "use_azure_cli": 'true',
                "azure_use_azure_cli": "true",
            }

    def _to_internal(self, url: str) -> str:
        """Convert URL to (filesystem, internal_path) for adlfs operations."""
        parsed = parse_url(url)
        return f"{parsed.bucket}/{parsed.path}"

    def _to_uri(self, account: str, internal_path: str) -> str:
        """Convert internal path back to az:// URL."""
        if self.url_authority_is_account:
            return f"az://{account}/{internal_path}"
        else:
            return f"az://{internal_path}"

    def open(self, url: str) -> BinaryIO:
        return self.fs.open(self._to_internal(url), "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        internal_path = self._to_internal(url)
        pattern = f"{internal_path}/**/*{extension}"
        for path in sorted(self.fs.glob(pattern)):
            info = self.fs.info(path)
            yield FileInfo(url=self._to_uri(self.account, path), size=info["size"])

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

    def __del__(self):
        try:
            # Check if fs exists and has a close method
            if hasattr(self, 'fs') and self.fs is not None:
                self.fs.close()
        except Exception:
            # Silently fail as the interpreter is likely in mid-teardown
            pass
