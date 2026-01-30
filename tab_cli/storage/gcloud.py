import os
from enum import Enum
from pathlib import Path

from typing import BinaryIO, Iterator
from loguru import logger

from tab_cli.storage.base import StorageBackend, FileInfo
from tab_cli.url_parser import parse_url


class GcloudAuthMethod(Enum):
    SERVICE_ACCOUNT = 1  # GOOGLE_APPLICATION_CREDENTIALS
    ADC = 2              # Application Default Credentials file
    GCLOUD_CLI = 3       # gcloud auth login (non-ADC)
    GOOGLE_DEFAULT = 4   # google.auth.default()


class GcloudBackend(StorageBackend):
    """Storage backend for Google Cloud Storage.

    URL format: gs://bucket/path

    Authentication order:
    1. GOOGLE_APPLICATION_CREDENTIALS environment variable (service account JSON)
    2. ADC file (~/.config/gcloud/application_default_credentials.json)
    3. google.auth.default() via gcsfs token="google_default"
    4. gcloud CLI login credentials (gcloud auth print-access-token)
    """

    def __init__(self) -> None:
        """Initialize the Google Cloud Storage backend."""
        try:
            import gcsfs
        except ImportError as e:
            raise ImportError("Package 'gcsfs' is required for gs:// URLs. Install with: pip install gcsfs") from e

        self.gcsfs = gcsfs
        self.fs = None

        # 1. Try GOOGLE_APPLICATION_CREDENTIALS environment variable
        credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_file and os.path.exists(credentials_file):
            logger.debug("Authenticating to GCS using GOOGLE_APPLICATION_CREDENTIALS: {}", credentials_file)
            try:
                self.fs = self.gcsfs.GCSFileSystem(token=credentials_file)
                self.method = GcloudAuthMethod.SERVICE_ACCOUNT
                self.token = credentials_file
                return
            except Exception as e:
                logger.debug("GOOGLE_APPLICATION_CREDENTIALS authentication failed: {}", e)

        # 2. Try ADC file (application_default_credentials.json)
        adc_path = self._get_adc_path()
        if adc_path and adc_path.exists():
            logger.debug("Authenticating to GCS using ADC file: {}", adc_path)
            try:
                self.fs = self.gcsfs.GCSFileSystem(token=str(adc_path))
                self.method = GcloudAuthMethod.ADC
                self.token = str(adc_path)
                return
            except Exception as e:
                logger.debug("ADC file authentication failed: {}", e)

        # 3. Fallback to gcloud CLI login (gcloud auth print-access-token)
        logger.debug("Attempting to get access token from gcloud CLI")
        access_token = self._get_access_token_via_cli()
        if access_token:
            logger.debug("Authenticating to GCS using gcloud CLI access token")
            try:
                self.fs = self.gcsfs.GCSFileSystem(token=access_token)
                self.method = GcloudAuthMethod.GCLOUD_CLI
                self.token = access_token
                return
            except Exception as e:
                logger.debug("gcloud CLI access token authentication failed: {}", e)

        # 4. Try google.auth.default() via token="google_default"
        logger.debug("Authenticating to GCS using google.auth.default()")
        try:
            self.fs = self.gcsfs.GCSFileSystem(token="google_default")
            self.method = GcloudAuthMethod.GOOGLE_DEFAULT
            self.token = "google_default"
            return
        except Exception as e:
            logger.debug("google.auth.default() authentication failed: {}", e)

        if self.fs is None:
            raise ValueError(
                "Could not authenticate to Google Cloud Storage. "
                "Set GOOGLE_APPLICATION_CREDENTIALS, run 'gcloud auth application-default login', "
                "or run 'gcloud auth login'."
            )

    def _get_adc_path(self) -> Path | None:
        """Get the path to the Application Default Credentials file."""
        # Check CLOUDSDK_CONFIG for custom config directory
        config_dir = os.environ.get("CLOUDSDK_CONFIG")
        if config_dir:
            return Path(config_dir) / "application_default_credentials.json"

        # Default locations
        if os.name == "nt":  # Windows
            appdata = os.environ.get("APPDATA")
            if appdata:
                return Path(appdata) / "gcloud" / "application_default_credentials.json"
        else:  # Linux/macOS
            home = Path.home()
            return home / ".config" / "gcloud" / "application_default_credentials.json"

        return None

    def _get_access_token_via_cli(self) -> str | None:
        """Get access token from gcloud CLI (gcloud auth print-access-token)."""
        import subprocess

        try:
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            logger.debug("gcloud CLI not available or timed out")
        return None

    def normalize_for_polars(self, url: str) -> str:
        """Normalize URL to a format Polars understands.

        Polars expects gs://bucket/path format.

        Returns:
            Normalized URL in gs://bucket/path format.
        """
        parsed = parse_url(url)
        return f"gs://{parsed.bucket}/{parsed.path}"

    def storage_options(self, url: str) -> dict[str, str]:
        """Return storage options for Polars GCS access.

        Returns:
            Dict with appropriate authentication options for GCS.
            Includes both gcsfs-style keys and Rust object_store keys for compatibility.
        """
        if self.method == GcloudAuthMethod.SERVICE_ACCOUNT:
            return {
                "token": self.token,
                "service_account": self.token,
                "google_service_account": self.token,
            }
        elif self.method == GcloudAuthMethod.ADC:
            return {
                "token": self.token,
                "service_account": self.token,
                "google_service_account": self.token,
            }
        elif self.method == GcloudAuthMethod.GCLOUD_CLI:
            # For CLI token, we need to refresh it for Polars
            # since the token may have expired
            fresh_token = self._get_access_token_via_cli()
            if fresh_token:
                return {
                    "token": fresh_token,
                }
            return {"token": self.token}
        elif self.method == GcloudAuthMethod.GOOGLE_DEFAULT:
            return {
                "token": "google_default",
            }

    def _to_internal(self, url: str) -> str:
        """Convert URL to internal path for gcsfs operations."""
        parsed = parse_url(url)
        return f"{parsed.bucket}/{parsed.path}"

    def _to_uri(self, internal_path: str) -> str:
        """Convert internal path back to gs:// URL."""
        return f"gs://{internal_path}"

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

    def __del__(self):
        try:
            if hasattr(self, 'fs') and self.fs is not None:
                # gcsfs doesn't have a close method, but clear session if possible
                if hasattr(self.fs, 'session') and self.fs.session:
                    pass  # aiohttp session cleanup is handled by gcsfs
        except Exception:
            pass
