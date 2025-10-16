"""
Freshness checking utilities for downloaded files.
Determines if a remote file is newer than the local version.
"""

from __future__ import annotations

import os
import hashlib
from typing import TYPE_CHECKING
from datetime import datetime
from email.utils import parsedate_to_datetime

if TYPE_CHECKING:
    from pathlib import Path

from . import _log


def check_freshness(
    local_path: str | Path,
    remote_headers: dict,
    local_metadata: dict | None = None,
    method: str = 'auto',
) -> tuple[bool, str]:
    """
    Check if local file is current compared to remote.

    Args:
        local_path: Path to local file
        remote_headers: HTTP headers from HEAD request to remote URL
        local_metadata: Stored metadata (ETag, Last-Modified, etc.)
        method: Check method - 'auto', 'etag', 'modified', 'hash', 'size'

    Returns:
        Tuple of (is_current, reason)
    """
    if not os.path.exists(local_path):
        return False, "local file does not exist"

    local_metadata = local_metadata or {}

    # Auto mode: try methods in order of reliability
    if method == 'auto':
        for check_method in ['etag', 'modified', 'size']:
            is_current, reason = _check_by_method(
                local_path, remote_headers, local_metadata, check_method
            )
            if reason != 'method_unavailable':
                return is_current, f"{check_method}: {reason}"
        return False, "no check method available"

    return _check_by_method(local_path, remote_headers, local_metadata, method)


def _check_by_method(
    local_path: str | Path,
    remote_headers: dict,
    local_metadata: dict,
    method: str,
) -> tuple[bool, str]:
    """Internal method dispatcher for freshness checks."""

    if method == 'etag':
        return _check_etag(remote_headers, local_metadata)
    elif method == 'modified':
        return _check_last_modified(remote_headers, local_metadata)
    elif method == 'hash':
        return _check_hash(local_path, remote_headers, local_metadata)
    elif method == 'size':
        return _check_size(local_path, remote_headers)
    else:
        return False, f"unknown method: {method}"


def _check_etag(remote_headers: dict, local_metadata: dict) -> tuple[bool, str]:
    """Check using ETag header."""
    remote_etag = remote_headers.get('ETag') or remote_headers.get('etag')
    local_etag = local_metadata.get('etag')

    if not remote_etag:
        return False, "method_unavailable"

    if not local_etag:
        return False, "no local etag stored"

    is_current = remote_etag == local_etag
    _log(f"ETag check: remote={remote_etag}, local={local_etag}, current={is_current}")

    return is_current, "etag match" if is_current else "etag mismatch"


def _check_last_modified(
    remote_headers: dict,
    local_metadata: dict,
) -> tuple[bool, str]:
    """Check using Last-Modified header."""
    remote_modified = (
        remote_headers.get('Last-Modified') or
        remote_headers.get('last-modified')
    )
    local_modified = local_metadata.get('last_modified')

    if not remote_modified:
        return False, "method_unavailable"

    if not local_modified:
        return False, "no local last-modified stored"

    try:
        remote_dt = parsedate_to_datetime(remote_modified)
        local_dt = parsedate_to_datetime(local_modified)

        is_current = local_dt >= remote_dt
        _log(
            f"Last-Modified check: remote={remote_dt}, "
            f"local={local_dt}, current={is_current}"
        )

        return is_current, "not modified" if is_current else "modified"
    except (ValueError, TypeError) as e:
        _log(f"Error parsing dates: {e}")
        return False, f"date parse error: {e}"


def _check_hash(
    local_path: str | Path,
    remote_headers: dict,
    local_metadata: dict,
) -> tuple[bool, str]:
    """Check using content hash (MD5 or SHA256)."""
    # Check for MD5 in headers
    remote_md5 = (
        remote_headers.get('Content-MD5') or
        remote_headers.get('content-md5')
    )

    if remote_md5:
        local_md5 = _compute_hash(local_path, 'md5')
        is_current = remote_md5 == local_md5
        _log(f"MD5 check: remote={remote_md5}, local={local_md5}, current={is_current}")
        return is_current, "md5 match" if is_current else "md5 mismatch"

    # Fallback to stored SHA256 if available
    local_sha256 = local_metadata.get('sha256')
    if local_sha256:
        # Note: We can't verify without downloading, so this is limited
        return False, "hash check requires download"

    return False, "method_unavailable"


def _check_size(local_path: str | Path, remote_headers: dict) -> tuple[bool, str]:
    """Check using Content-Length header (least reliable)."""
    remote_size = (
        remote_headers.get('Content-Length') or
        remote_headers.get('content-length')
    )

    if not remote_size:
        return False, "method_unavailable"

    try:
        remote_size = int(remote_size)
        local_size = os.path.getsize(local_path)

        is_current = local_size == remote_size
        _log(
            f"Size check: remote={remote_size}, "
            f"local={local_size}, current={is_current}"
        )

        return is_current, "size match" if is_current else "size mismatch"
    except (ValueError, OSError) as e:
        _log(f"Error checking size: {e}")
        return False, f"size check error: {e}"


def _compute_hash(file_path: str | Path, algorithm: str = 'sha256') -> str:
    """Compute hash of local file."""
    h = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_remote_headers(url: str, **kwargs) -> dict:
    """
    Perform HEAD request to get remote headers without downloading.

    Args:
        url: URL to check
        **kwargs: Additional arguments (timeout, headers, etc.)

    Returns:
        Dictionary of response headers
    """
    import requests

    try:
        response = requests.head(url, allow_redirects=True, **kwargs)
        _log(f"HEAD request to {url}: status={response.status_code}")
        return dict(response.headers)
    except Exception as e:
        _log(f"Error getting remote headers: {e}")
        return {}
