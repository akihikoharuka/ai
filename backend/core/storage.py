"""Storage abstraction. Local filesystem now, Azure Blob later."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


class LocalStorage:
    """Local filesystem storage backend."""

    def __init__(self, base_dir: str = "output"):
        self.base_dir = os.path.abspath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)

    def save(self, path: str, data: bytes) -> str:
        """Save data to a file path relative to base_dir."""
        full_path = os.path.join(self.base_dir, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(data)
        return full_path

    def load(self, path: str) -> bytes:
        """Load data from a file path relative to base_dir."""
        full_path = os.path.join(self.base_dir, path)
        with open(full_path, "rb") as f:
            return f.read()

    def exists(self, path: str) -> bool:
        """Check if a file exists."""
        full_path = os.path.join(self.base_dir, path)
        return os.path.exists(full_path)

    def list_files(self, prefix: str = "") -> list[str]:
        """List files under a prefix."""
        search_dir = os.path.join(self.base_dir, prefix)
        if not os.path.exists(search_dir):
            return []
        files = []
        for root, _, filenames in os.walk(search_dir):
            for filename in filenames:
                rel = os.path.relpath(os.path.join(root, filename), self.base_dir)
                files.append(rel)
        return files

    def delete(self, path: str) -> None:
        """Delete a file or directory."""
        full_path = os.path.join(self.base_dir, path)
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)
        elif os.path.exists(full_path):
            os.remove(full_path)

    def get_full_path(self, path: str) -> str:
        """Get absolute path for a relative path."""
        return os.path.join(self.base_dir, path)
