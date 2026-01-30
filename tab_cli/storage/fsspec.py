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

from tab_cli.storage.base import FileInfo, StorageBackend

# Package hints for common protocols
_PACKAGE_HINTS: dict[str, str] = {}


class FsspecBackend(StorageBackend):

    def __init__(self, protocol: str) -> None:
        self._protocol = protocol

        try:
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
        return None
