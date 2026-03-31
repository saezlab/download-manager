# download-manager — Instructions for AI Assistants

You are working in the **download-manager** package, a Python download manager
providing a common API for `pycurl` and `requests` backends, integrated with
`cache-manager` for local file caching.

## About this package

download-manager handles HTTP downloads with configurable backends (pycurl or
requests), automatic caching through cache-manager, progress bars, multipart
uploads, and descriptor-based request configuration. It is used by other
saezlab packages (notably pypath and omnipath-client) for all data
acquisition.

## Architecture

- `_descriptor.py` — `Descriptor` class: a dict-like container for all
  download parameters (URL, headers, POST data, multipart, etc.)
- `_downloader.py` — `AbstractDownloader` base class with `CurlDownloader`
  and `RequestsDownloader` implementations
- `_manager.py` — `DownloadManager`: top-level API, orchestrates descriptors,
  downloaders, and cache integration
- `_curlopt.py` — PyCurl option processing and synonyms
- `_session.py` — `pypath_common` session initialization
- `_data/` — Packaged data loading via `pypath_common`
- `_misc.py` — Utilities: `file_digest`, `parse_header`
- `_metadata.py` — Version and author metadata from `pyproject.toml`
- `_constants.py` — Cache key constants

## Logging

This package uses the standard library `logging` module. Each module creates
its own logger via `logging.getLogger(__name__)` with a `NullHandler`. Do
**not** use the legacy `_log()` / `session._logger.msg` pattern — that has
been removed in favour of `logger.debug()` / `logger.info()` / etc.

## Dependencies (our packages)

- **pypath-common** (`pypath_common`) — session management, data loading,
  misc utilities
- **cache-manager** (`cache_manager`) — local file caching, cache item
  lifecycle, file opening

## Coding conventions

Follow the saezlab Python coding style documented in the architecture
repository at `~/saezverse` (specifically
`human/guidelines/python-coding-style.md`). Key points:

- Spaces around `=` in keyword arguments and default values
- Blank lines inside functions around blocks and logical segments
- Single quotes for strings
- Napoleon (Google) docstring style

## Cross-project context

The architecture repository at `~/saezverse` contains package descriptions,
coding conventions, architecture decisions, and development plans for all
saezlab packages. Consult it for:

- How download-manager fits into the broader package portfolio
- Cross-package API contracts (especially with cache-manager and pypath)
- Coding style guidelines and ADRs
