"""Cloud storage backend using fsspec.

Supports any protocol that fsspec supports, including:
- s3:// - AWS S3 (requires s3fs)
- gs:// - Google Cloud Storage (requires gcsfs)
- abfss:// - Azure Data Lake Storage Gen2 (requires adlfs)

- az:// is delegated to AzBackend for custom handling.

The appropriate protocol handler package must be installed separately.
"""

from typing import BinaryIO, Iterator, Any
import fsspec

from tab_cli.storage.base import FileInfo, StorageBackend

# Package hints for common protocols
_PACKAGE_HINTS = {
    "s3": "s3fs",
    "gs": "gcsfs",
    "gcs": "gcsfs",
    "abfs": "adlfs",
    "abfss": "adlfs",
}


class FsspecBackend(StorageBackend):

    def __init__(self, protocol: str) -> None:
        self._protocol = protocol

        # TODO: isolate gs
        if protocol == "gs":
            # Google Cloud Storage needs a token for Polars access
            self._gcs_token = self._get_gcs_token()
        try:
            if protocol == "gs":
                self._fs: fsspec.AbstractFileSystem = fsspec.filesystem(protocol, token=self._gcs_token)
            else:
                self._fs: fsspec.AbstractFileSystem = fsspec.filesystem(protocol)
        except (ImportError, ValueError) as e:
            pkg = _PACKAGE_HINTS.get(protocol)
            if pkg is not None:
                raise ImportError(f"Package '{pkg}' is required for {protocol}:// URLs. Install with: pip install {pkg}") from e
            raise ImportError(f"No handler found for {protocol}:// URLs") from e

    def open(self, url: str) -> BinaryIO:
        return self._fs.open(url, "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        pattern = f"{url}/**/*{extension}"
        for path in sorted(self._fs.glob(pattern)):
            info = self._fs.info(path)
            full_uri = f"{self._protocol}://{path}" if not path.startswith(f"{self._protocol}://") else path
            yield FileInfo(url=full_uri, size=info["size"])

    def size(self, url: str) -> int:
        return self._fs.size(url)

    def is_directory(self, url: str) -> bool:
        try:
            info = self._fs.info(url)
            return info.get("type") == "directory"
        except FileNotFoundError:
            try:
                contents = self._fs.ls(url, detail=False)
                return len(contents) > 0
            except Exception:
                return False

    def storage_options(self, url: str) -> dict[str, Any] | None:
        if self._protocol in ("gs", "gcs"):
            return {"token": self._get_gcs_token()}
        return None

    def _get_gcs_token(self) -> str | None:
        """Get GCS access token from gcloud CLI."""
        import subprocess
        try:
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                self._gcs_token = result.stdout.strip()
                return self._gcs_token
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None
