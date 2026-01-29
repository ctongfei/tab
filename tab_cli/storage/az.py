import os

from typing import TYPE_CHECKING, Any, BinaryIO, Iterator
from urllib.parse import urlparse

from tab_cli.storage import StorageBackend, FileInfo

if TYPE_CHECKING:
    import adlfs


class AzBackend(StorageBackend):
    """Storage backend for Azure Blob Storage with configurable URL interpretation.

    When az_url_authority_is_account=True:
        - az://account/container/path - authority is the storage account name
        - az:///container/path - account inferred from AZURE_STORAGE_ACCOUNT

    When az_url_authority_is_account=False (default adlfs behavior):
        - az://container/path - authority is the container name
    """

    def __init__(self, az_url_authority_is_account: bool = False) -> None:
        """Initialize the Azure Blob Storage backend.

        Args:
            az_url_authority_is_account: If True, interpret the URL authority as the
                storage account name. If False, interpret it as the container name.
        """
        try:
            import adlfs
        except ImportError as e:
            raise ImportError("Package 'adlfs' is required for az:// URLs. Install with: pip install adlfs") from e

        self._adlfs = adlfs
        self._url_authority_is_account = az_url_authority_is_account
        self._fs_cache: dict[str, adlfs.AzureBlobFileSystem] = {}

    def _parse_url(self, url: str) -> tuple[str, str]:
        """Parse az:// URL and return (account_name, internal_path).

        Returns:
            Tuple of (account_name, path_for_adlfs) where path_for_adlfs is
            in the format that adlfs expects (container/path).

        Raises:
            ValueError: If account name cannot be determined.
        """
        parsed = urlparse(url)

        if self._url_authority_is_account:
            # Authority is the account name
            # az://account/container/path or az:///container/path
            account = parsed.netloc if parsed.netloc else None
            if account is None:
                # Infer from environment
                account = os.environ.get("AZURE_STORAGE_ACCOUNT")
                if account is None:
                    raise ValueError(
                        "No storage account specified in URL and AZURE_STORAGE_ACCOUNT not set. "
                        "Use az://account/container/path and use `tab --az-url-authority-is-account`, or set the environment variable."
                    )
            # Path is /container/path, strip leading slash
            internal_path = parsed.path.lstrip("/")
        else:
            # Authority is the container name (default adlfs behavior)
            # az://container/path
            account = os.environ.get("AZURE_STORAGE_ACCOUNT")
            if account is None:
                raise ValueError(
                    "AZURE_STORAGE_ACCOUNT environment variable not set. "
                    "Use az://account/container/path and use `tab --az-url-authority-is-account`, or set the environment variable."
                )
            internal_path = f"{parsed.netloc}{parsed.path}"

        return account, internal_path

    def _get_fs(self, account: str) -> "adlfs.AzureBlobFileSystem":
        """Get or create filesystem for the given account.

        Tries authentication in order:
        1. AZURE_STORAGE_KEY environment variable
        2. Account key fetched via Azure CLI (az storage account keys list)
        3. DefaultAzureCredential (Azure AD / RBAC) - requires Storage Blob Data Reader role
        """
        if account in self._fs_cache:
            return self._fs_cache[account]

        # 1. Try account key from environment
        account_key = os.environ.get("AZURE_STORAGE_KEY")
        if account_key:
            self._fs_cache[account] = self._adlfs.AzureBlobFileSystem(
                account_name=account,
                account_key=account_key,
            )
            return self._fs_cache[account]

        # 2. Try fetching account key via Azure CLI (works if user has ARM access via az login)
        account_key = self._get_account_key_via_cli(account)
        if account_key:
            self._fs_cache[account] = self._adlfs.AzureBlobFileSystem(
                account_name=account,
                account_key=account_key,
            )
            return self._fs_cache[account]

        # 3. Fallback to DefaultAzureCredential (requires Storage Blob Data Reader RBAC role)
        try:
            from azure.identity import DefaultAzureCredential

            self._fs_cache[account] = self._adlfs.AzureBlobFileSystem(
                account_name=account,
                credential=DefaultAzureCredential(),  # type: ignore[arg-type]
            )
            return self._fs_cache[account]
        except ImportError:
            pass

        raise ValueError(
            f"Could not authenticate to storage account '{account}'. "
            "Set AZURE_STORAGE_KEY, run 'az login', or configure Azure AD RBAC."
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
        account, internal_path = self._parse_url(url)
        return f"az://{internal_path}"

    def storage_options(self, url: str) -> dict[str, str] | None:
        """Return storage options for Polars Azure access.

        Returns:
            Dict with account_name and account_key for Azure authentication
        """
        account, _ = self._parse_url(url)

        # Try to get account key
        account_key = os.environ.get("AZURE_STORAGE_KEY")
        if not account_key:
            account_key = self._get_account_key_via_cli(account)

        if account_key:
            return {
                "account_name": account,
                "account_key": account_key,
            }

        return {"account_name": account}

    def _to_internal(self, url: str) -> tuple[Any, str]:
        """Convert URL to (filesystem, internal_path) for adlfs operations."""
        account, internal_path = self._parse_url(url)
        fs = self._get_fs(account)
        return fs, internal_path

    def _to_uri(self, account: str, internal_path: str) -> str:
        """Convert internal path back to az:// URL."""
        if self._url_authority_is_account:
            return f"az://{account}/{internal_path}"
        else:
            return f"az://{internal_path}"

    def open(self, url: str) -> BinaryIO:
        fs, path = self._to_internal(url)
        return fs.open(path, "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        account, internal_path = self._parse_url(url)
        fs = self._get_fs(account)
        pattern = f"{internal_path}/**/*{extension}"
        for path in sorted(fs.glob(pattern)):
            info = fs.info(path)
            yield FileInfo(url=self._to_uri(account, path), size=info["size"])

    def size(self, url: str) -> int:
        fs, path = self._to_internal(url)
        return fs.size(path)

    def is_directory(self, url: str) -> bool:
        fs, path = self._to_internal(url)
        try:
            info = fs.info(path)
            return info.get("type") == "directory"
        except FileNotFoundError:
            try:
                contents = fs.ls(path, detail=False)
                return len(contents) > 0
            except Exception:
                return False
