#!/usr/bin/env python
"""
Standalone freshness checker for downloaded files.

Scans all downloaded files and generates a report on their freshness status.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Generator
from datetime import datetime
from dataclasses import dataclass

from . import _freshness


@dataclass
class FreshnessReport:
    """Report for a single file's freshness status."""

    module: str
    filename: str
    local_path: Path
    status: str  # 'current', 'outdated', 'unknown', 'unable_to_check'
    reason: str
    method_used: str | None
    url: str | None
    download_method: str | None
    downloaded_at: str | None
    size: int | None
    sha256: str | None


def scan_data_folder(data_folder: str | Path) -> Generator[FreshnessReport, None, None]:
    """
    Scan data folder and check freshness of all downloaded files.

    Args:
        data_folder: Base data folder containing module subfolders

    Yields:
        FreshnessReport for each file found
    """
    data_path = Path(data_folder)

    if not data_path.exists():
        return

    # Iterate through module folders
    for module_folder in data_path.iterdir():
        if not module_folder.is_dir():
            continue

        module_name = module_folder.name
        metadata_folder = module_folder / '.metadata'

        if not metadata_folder.exists():
            continue

        # Iterate through metadata files
        for meta_file in metadata_folder.glob('*.meta'):
            # Get corresponding data file
            filename = meta_file.stem  # Remove .meta extension
            file_path = module_folder / filename

            if not file_path.exists():
                continue

            # Load metadata
            try:
                with open(meta_file, 'r') as f:
                    metadata = json.load(f)
            except (json.JSONDecodeError, OSError):
                yield FreshnessReport(
                    module=module_name,
                    filename=filename,
                    local_path=file_path,
                    status='unknown',
                    reason='failed to load metadata',
                    method_used=None,
                    url=None,
                    download_method=None,
                    downloaded_at=None,
                    size=None,
                    sha256=None,
                )
                continue

            # Check freshness
            report = check_file_freshness(
                module_name,
                filename,
                file_path,
                metadata,
            )

            yield report


def check_file_freshness(
    module: str,
    filename: str,
    file_path: Path,
    metadata: dict,
) -> FreshnessReport:
    """
    Check freshness of a single file.

    Args:
        module: Module name
        filename: File name
        file_path: Path to the file
        metadata: Metadata dictionary

    Returns:
        FreshnessReport for the file
    """
    url = metadata.get('url')
    method = metadata.get('method', 'GET')

    # Base report info
    report_base = {
        'module': module,
        'filename': filename,
        'local_path': file_path,
        'url': url,
        'download_method': method,
        'downloaded_at': metadata.get('downloaded_at'),
        'size': metadata.get('size'),
        'sha256': metadata.get('sha256'),
    }

    # Can't check POST requests
    if method == 'POST':
        return FreshnessReport(
            **report_base,
            status='unable_to_check',
            reason='POST request (cannot check freshness)',
            method_used=None,
        )

    # Need URL to check
    if not url:
        return FreshnessReport(
            **report_base,
            status='unknown',
            reason='no URL in metadata',
            method_used=None,
        )

    # Get remote headers
    remote_headers = _freshness.get_remote_headers(url, timeout=30)

    if not remote_headers:
        return FreshnessReport(
            **report_base,
            status='unknown',
            reason='failed to get remote headers',
            method_used=None,
        )

    # Check freshness
    is_current, reason = _freshness.check_freshness(
        file_path,
        remote_headers,
        metadata,
        method='auto',
    )

    # Extract method used from reason
    method_used = reason.split(':')[0] if ':' in reason else None

    return FreshnessReport(
        **report_base,
        status='current' if is_current else 'outdated',
        reason=reason,
        method_used=method_used,
    )


def print_report(reports: list[FreshnessReport], verbose: bool = False) -> None:
    """
    Print a formatted freshness report.

    Args:
        reports: List of FreshnessReport objects
        verbose: If True, show all details
    """
    print("\n" + "=" * 80)
    print("DOWNLOAD FRESHNESS REPORT")
    print("=" * 80)

    by_module = {}
    for report in reports:
        if report.module not in by_module:
            by_module[report.module] = []
        by_module[report.module].append(report)

    for module, module_reports in sorted(by_module.items()):
        print(f"\n📁 Module: {module}")
        print("-" * 80)

        for report in module_reports:
            # Status icon
            if report.status == 'current':
                icon = '✅'
            elif report.status == 'outdated':
                icon = '🔄'
            elif report.status == 'unable_to_check':
                icon = '⚠️ '
            else:
                icon = '❓'

            print(f"\n  {icon} {report.filename}")

            if verbose or report.status != 'current':
                if report.downloaded_at:
                    try:
                        dt = datetime.fromisoformat(report.downloaded_at)
                        downloaded = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        downloaded = report.downloaded_at
                    print(f"     Downloaded: {downloaded}")

                if report.size:
                    size_mb = report.size / (1024 * 1024)
                    print(f"     Size: {size_mb:.2f} MB")

                if report.download_method:
                    print(f"     Method: {report.download_method}")

                if report.url and verbose:
                    print(f"     URL: {report.url}")

            # Freshness status
            if report.status == 'current':
                print(f"     Status: ✓ Current ({report.reason})")
            elif report.status == 'outdated':
                print(f"     Status: ✗ Outdated ({report.reason})")
            elif report.status == 'unable_to_check':
                print(f"     Status: ⚠ {report.reason}")
            else:
                print(f"     Status: ? {report.reason}")

    # Summary
    print("\n" + "=" * 80)
    total = len(reports)
    current = sum(1 for r in reports if r.status == 'current')
    outdated = sum(1 for r in reports if r.status == 'outdated')
    unable = sum(1 for r in reports if r.status == 'unable_to_check')
    unknown = sum(1 for r in reports if r.status == 'unknown')

    print(f"SUMMARY: {total} files total")
    print(f"  ✅ Current: {current}")
    print(f"  🔄 Outdated: {outdated}")
    print(f"  ⚠️  Unable to check: {unable}")
    print(f"  ❓ Unknown: {unknown}")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    import sys

    data_folder = sys.argv[1] if len(sys.argv) > 1 else './data'
    verbose = '--verbose' in sys.argv or '-v' in sys.argv

    print(f"Scanning data folder: {data_folder}")
    reports = list(scan_data_folder(data_folder))
    print_report(reports, verbose=verbose)
