from __future__ import annotations

from pathlib import Path

from app.core.config import get_settings


def get_raw_archive_root() -> Path:
    return Path(get_settings().raw_archive_root).expanduser().resolve()


def write_raw_archive(storage_path: str, content: bytes) -> Path:
    archive_root = get_raw_archive_root()
    destination = archive_root / storage_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return destination


def read_raw_archive(storage_path: str) -> bytes:
    archive_root = get_raw_archive_root()
    return (archive_root / storage_path).read_bytes()
