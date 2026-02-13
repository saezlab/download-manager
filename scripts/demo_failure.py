#!/usr/bin/env python3
"""Run failure-path demos for download_manager with verbose logging."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow running as: python scripts/demo_failure.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import download_manager as dm


def configure_logging() -> None:
    """Enable built-in logging for the whole process."""

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)5s] [%(name)s:%(lineno)d] %(message)s",
    )
    logging.getLogger("download_manager").setLevel(logging.DEBUG)


def main() -> int:
    configure_logging()
    logger = logging.getLogger("demo.failure")

    cache_dir = PROJECT_ROOT / "demo_cache_fail"
    manager = dm.DownloadManager(path=str(cache_dir), backend="requests")

    logger.info("=== FAILURE DEMO START ===")

    logger.info("1) Server-side failure with retries (HTTP 500)")
    fail_status_url = "https://httpbin.org/status/500"
    desc, item, downloader, _ = manager._download(
        fail_status_url,
        dest=False,
        retries=2,
    )
    logger.info(
        "HTTP 500 demo completed. url=%s ok=%s http_code=%s cache_key=%s",
        desc["url"],
        downloader.ok,
        downloader.http_code,
        getattr(item, "key", None),
    )

    logger.info("2) Transport/DNS failure (exception path)")
    fail_network_url = "https://non-existent-download-manager-demo.invalid/file.txt"
    try:
        manager.download(fail_network_url, dest=False, timeout=2, connecttimeout=2)
    except Exception as exc:
        logger.exception(
            "Expected network failure happened for URL=%s (%s)",
            fail_network_url,
            type(exc).__name__,
        )

    logger.info("=== FAILURE DEMO END ===")
    logger.info("Tip: existing _log messages (from pypath/cache_manager) run in parallel.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
