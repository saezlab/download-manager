"""
Metadata storage for downloaded files.
Stores ETags, Last-Modified dates, checksums, etc. in .metadata folder.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Any

from . import _log


class MetadataStore:
    """
    Manages metadata for downloaded files.

    Stores metadata in .metadata/{filename}.meta as JSON alongside the file.
    """

    def __init__(self, data_folder: Path):
        """
        Initialize metadata store.

        Args:
            data_folder: Base data folder
        """
        self.data_folder = data_folder

    def get_metadata_path(self, file_path: Path) -> Path:
        """
        Get path to metadata file for given file.

        Args:
            file_path: Path to the actual file

        Returns:
            Path to the metadata file in the same directory as the file.
        """

        # Get the directory containing the file
        file_dir = file_path.parent

        # Create .metadata folder in the same directory
        metadata_folder = file_dir / '.metadata'
        metadata_folder.mkdir(parents=True, exist_ok=True)

        return metadata_folder / f"{file_path.name}.meta"

    def load(self, file_path: Path) -> dict:
        """
        Load metadata for a file.

        Args:
            file_path: Path to the file

        Returns:
            Dictionary with metadata, or empty dict if not found
        """
        meta_path = self.get_metadata_path(file_path)

        if not meta_path.exists():
            _log(f"No metadata found for {file_path}")
            return {}

        try:
            with open(meta_path, 'r') as f:
                metadata = json.load(f)
            _log(f"Loaded metadata for {file_path}: {list(metadata.keys())}")
            return metadata
        except (json.JSONDecodeError, OSError) as e:
            _log(f"Error loading metadata for {file_path}: {e}")
            return {}

    def save(self, file_path: Path, metadata: dict) -> None:
        """
        Save metadata for a file.

        Args:
            file_path: Path to the file
            metadata: Dictionary with metadata to save
        """
        meta_path = self.get_metadata_path(file_path)

        # Add timestamp
        metadata['_updated'] = datetime.now().isoformat()

        try:
            with open(meta_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            _log(f"Saved metadata for {file_path}: {list(metadata.keys())}")
        except OSError as e:
            _log(f"Error saving metadata for {file_path}: {e}")

    def update(self, file_path: Path, **kwargs) -> None:
        """
        Update metadata for a file (merges with existing).

        Args:
            file_path: Path to the file
            **kwargs: Metadata fields to update
        """
        metadata = self.load(file_path)
        metadata.update(kwargs)
        self.save(file_path, metadata)

    def delete(self, file_path: Path) -> None:
        """
        Delete metadata for a file.

        Args:
            file_path: Path to the file
        """
        meta_path = self.get_metadata_path(file_path)

        if meta_path.exists():
            try:
                meta_path.unlink()
                _log(f"Deleted metadata for {file_path}")
            except OSError as e:
                _log(f"Error deleting metadata for {file_path}: {e}")

    def save_from_headers(
        self,
        file_path: Path,
        headers: dict,
        sha256: str | None = None,
        size: int | None = None,
        url: str | None = None,
        method: str | None = None,
        post_data: dict | None = None,
        query_params: dict | None = None,
    ) -> None:
        """
        Save metadata extracted from response headers and download details.

        Args:
            file_path: Path to the file
            headers: Response headers dict
            sha256: SHA256 checksum of the file
            size: File size in bytes
            url: The URL the file was downloaded from
            method: HTTP method used (GET, POST)
            post_data: POST data if applicable
            query_params: Query parameters if applicable
        """
        metadata = {}

        # Download details
        if url:
            metadata['url'] = url
        if method:
            metadata['method'] = method
        if post_data:
            metadata['post_data'] = post_data
        if query_params:
            metadata['query_params'] = query_params

        # Extract ETag
        if etag := headers.get('ETag') or headers.get('etag'):
            metadata['etag'] = etag

        # Extract Last-Modified
        if last_modified := headers.get('Last-Modified') or headers.get('last-modified'):
            metadata['last_modified'] = last_modified

        # Extract Content-MD5
        if content_md5 := headers.get('Content-MD5') or headers.get('content-md5'):
            metadata['content_md5'] = content_md5

        # Add checksum if provided
        if sha256:
            metadata['sha256'] = sha256

        # Add size if provided
        if size:
            metadata['size'] = size

        # Add download timestamp
        metadata['downloaded_at'] = datetime.now().isoformat()

        self.save(file_path, metadata)
