#!/usr/bin/env python3
"""Run a success-path demo for download_manager with verbose logging."""

from __future__ import annotations

import io
import logging
import sys
from pathlib import Path

# Allow running as: python scripts/demo_success.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import download_manager as dm

def main() -> int:

    # Import saezlab_core library
    import saezlab_core

    # Create a unique session
    session = saezlab_core.get_session("./")

    # Get a logger
    logger = session.get_logger()
    logger.info("This is an info message!")

    url = "https://example-files.online-convert.com/document/txt/example.txt"
    cache_dir = PROJECT_ROOT / "demo_cache"
    out_file = PROJECT_ROOT / "example.txt"

    logger.info("=== SUCCESS DEMO START ===")
    logger.info("URL=%s", url)

    manager = dm.DownloadManager(path=str(cache_dir), backend="requests")

    logger.info("1) Downloading to in-memory buffer")
    buffer = manager.download(url, dest=False)
    if isinstance(buffer, io.BytesIO):
        preview = buffer.getvalue()[:100].decode("utf-8", errors="replace")
        logger.info("Buffer download succeeded. Preview=%r", preview)
    else:
        logger.warning("Unexpected buffer type: %s", type(buffer).__name__)

    logger.info("2) Downloading to explicit file path")
    file_path = manager.download(url, dest=str(out_file))
    logger.info("File download result path=%s", file_path)

    logger.info("3) Cache-first download (dest=None)")
    cached_path = manager.download(url)
    logger.info("Cache-aware download path=%s", cached_path)

    logger.info("=== SUCCESS DEMO END ===")
    logger.info("Tip: existing _log messages (from pypath/cache_manager) run in parallel.")
    return 0


if __name__ == "__main__":
    main()
