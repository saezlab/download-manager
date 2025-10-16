#!/usr/bin/env python
"""
CLI entry point for download_manager freshness checker.

Usage:
    python -m download_manager check <data_folder> [--verbose]
"""

import sys
import argparse
from pathlib import Path

from .check_freshness import scan_data_folder, print_report


def main():
    parser = argparse.ArgumentParser(
        description='Check freshness of downloaded files'
    )
    parser.add_argument(
        'command',
        choices=['check'],
        help='Command to run'
    )
    parser.add_argument(
        'data_folder',
        nargs='?',
        default='./data',
        help='Data folder to scan (default: ./data)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show verbose output'
    )

    args = parser.parse_args()

    if args.command == 'check':
        data_path = Path(args.data_folder)
        if not data_path.exists():
            print(f"Error: Data folder '{args.data_folder}' does not exist")
            sys.exit(1)

        print(f"Scanning data folder: {args.data_folder}")
        reports = list(scan_data_folder(args.data_folder))

        if not reports:
            print("No downloaded files found.")
            sys.exit(0)

        print_report(reports, verbose=args.verbose)


if __name__ == '__main__':
    main()
