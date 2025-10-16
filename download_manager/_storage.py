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

    Stores metadata in .metadata/{filename}.meta as JSON.
    """

    def __init__(self, data_folder: str | Path, module_name: str):
        """
        Initialize metadata store.

        Args:
            data_folder: Base data folder
            module_name: Module subfolder name
        """
        self.data_folder = Path(data_folder)
        self.module_name = module_name
        self.module_folder = self.data_folder / module_name
        self.metadata_folder = self.module_folder / '.metadata'

        # Create folders if they don't exist
        self.metadata_folder.mkdir(parents=True, exist_ok=True)

    def get_metadata_path(self, filename: str) -> Path:
        """Get path to metadata file for given filename."""
        return self.metadata_folder / f"{filename}.meta"

    def load(self, filename: str) -> dict:
        """
        Load metadata for a file.

        Args:
            filename: Name of the file

        Returns:
            Dictionary with metadata, or empty dict if not found
        """
        meta_path = self.get_metadata_path(filename)

        if not meta_path.exists():
            _log(f"No metadata found for {filename}")
            return {}

        try:
            with open(meta_path, 'r') as f:
                metadata = json.load(f)
            _log(f"Loaded metadata for {filename}: {list(metadata.keys())}")
            return metadata
        except (json.JSONDecodeError, OSError) as e:
            _log(f"Error loading metadata for {filename}: {e}")
            return {}

    def save(self, filename: str, metadata: dict) -> None:
        """
        Save metadata for a file.

        Args:
            filename: Name of the file
            metadata: Dictionary with metadata to save
        """
        meta_path = self.get_metadata_path(filename)

        # Add timestamp
        metadata['_updated'] = datetime.now().isoformat()

        try:
            with open(meta_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            _log(f"Saved metadata for {filename}: {list(metadata.keys())}")
        except OSError as e:
            _log(f"Error saving metadata for {filename}: {e}")

    def update(self, filename: str, **kwargs) -> None:
        """
        Update metadata for a file (merges with existing).

        Args:
            filename: Name of the file
            **kwargs: Metadata fields to update
        """
        metadata = self.load(filename)
        metadata.update(kwargs)
        self.save(filename, metadata)

    def delete(self, filename: str) -> None:
        """
        Delete metadata for a file.

        Args:
            filename: Name of the file
        """
        meta_path = self.get_metadata_path(filename)

        if meta_path.exists():
            try:
                meta_path.unlink()
                _log(f"Deleted metadata for {filename}")
            except OSError as e:
                _log(f"Error deleting metadata for {filename}: {e}")

    def save_from_headers(
        self,
        filename: str,
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
            filename: Name of the file
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

        self.save(filename, metadata)
