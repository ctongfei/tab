"""Cloud storage backend using fsspec.

Fallback for any fsspec-supported protocol not handled by dedicated backends.

Note: The following have dedicated backends:
      - az://, abfs://, abfss:// -> AzBackend
      - gs:// -> GcsBackend
      - s3:// -> S3Backend

The appropriate protocol handler package must be installed separately.
"""

from typing import BinaryIO, Iterator, Any
import fsspec
from loguru import logger

from tab_cli.storage.base import FileInfo, StorageBackend, has_glob_pattern
from tab_cli.url_parser import parse_url


class FsspecBackend(StorageBackend):
    """Base class for fsspec-based storage backends.

    This class provides common implementations for fsspec-compatible filesystems.
    Subclasses can override _to_internal and _to_uri to customize path handling.
    """

    fs: fsspec.AbstractFileSystem | None
    protocol: str

    def __init__(self, protocol: str) -> None:
        self.protocol = protocol

        try:
            self.fs = fsspec.filesystem(protocol)
        except (ImportError, ValueError) as e:
            raise ImportError(f"No handler found for {protocol}:// URLs") from e

    def _to_internal(self, url: str) -> str:
        """Convert URL to internal path for fsspec operations.

        Default implementation returns the URL as-is.
        Cloud backends override this to return bucket/path format.
        """
        return url

    def _to_uri(self, internal_path: str) -> str:
        """Convert internal path back to a URL.

        Default implementation prefixes with protocol if needed.
        """
        if internal_path.startswith(f"{self.protocol}://"):
            return internal_path
        return f"{self.protocol}://{internal_path}"

    def _require_fs(self) -> fsspec.AbstractFileSystem:
        if self.fs is None:
            raise RuntimeError(f"Filesystem for {self.protocol}:// is not initialized")
        return self.fs

    def open(self, url: str) -> BinaryIO:
        return self._require_fs().open(self._to_internal(url), "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        internal_path = self._to_internal(url)
        is_glob = has_glob_pattern(url)
        if is_glob:
            pattern = internal_path
        elif extension:
            pattern = f"{internal_path}/**/*.{extension}"
        else:
            pattern = f"{internal_path}/**/*"
        files = self._require_fs().glob(pattern)
        logger.debug(f"{len(files)} files found.")
        for path in sorted(files):
            if not is_glob and extension and not path.endswith(f".{extension}"):
                continue
            info = self._require_fs().info(path)
            yield FileInfo(url=self._to_uri(path), size=info["size"])

    def size(self, url: str) -> int:
        return self._require_fs().size(self._to_internal(url))

    def is_directory(self, url: str) -> bool:
        path = self._to_internal(url)
        try:
            info = self._require_fs().info(path)
            return info.get("type") == "directory"
        except FileNotFoundError:
            try:
                contents = self._require_fs().ls(path, detail=False)
                return len(contents) > 0
            except Exception:
                return False

    def storage_options(self, url: str) -> dict[str, Any] | None:
        return None


class CloudFsspecBackend(FsspecBackend):
    """Base class for cloud storage backends (S3, Azure, GCS).

    Cloud backends use bucket/path format internally instead of full URLs.
    """

    def __init__(self) -> None:
        # Subclasses must set self.fs and self.protocol before calling methods
        pass

    def _to_internal(self, url: str) -> str:
        """Convert URL to internal path (bucket/path) for cloud fsspec operations."""
        parsed = parse_url(url.rstrip("/"))  # strip trailing slash to avoid empty path segments
        return f"{parsed.bucket}/{parsed.path}"

    def _to_uri(self, internal_path: str) -> str:
        """Convert internal path back to URL. Subclasses must implement."""
        return f"{self.protocol}://{internal_path}"
