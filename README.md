[![Tests][badge-ci]][link-ci]
[![Coverage][badge-cov]][link-cov]

[badge-cov]: https://codecov.io/github/saezlab/download-manager/graph/badge.svg
[link-cov]: https://codecov.io/github/saezlab/download-manager
[badge-ci]: https://img.shields.io/github/actions/workflow/status/saezlab/download-manager/ci.yml?branch=main
[link-ci]: https://github.com/saezlab/download-manager/actions/workflows/ci.yml

# A Download Manager Python Module

A flexible, cache-aware download manager for Python, supporting multiple backends (requests, pycurl), with integrated caching and metadata management.

---

## Features

- **Multiple Backends:** Choose between `requests` and `pycurl` for downloads.
- **Cache Integration:** Seamless integration with [`cache-manager`](https://github.com/saezlab/cache-manager) for efficient file reuse and metadata tracking.
- **Flexible Destinations:** Download to disk, in-memory buffer, or cache.
- **Automatic Metadata:** Tracks download status, timestamps, HTTP headers, file hashes, and more.
- **Configurable:** Supports configuration via Python dict or config file.
- **Pre-commit, Linting, and CI:** Ready for robust development workflows.

---

## Installation

```bash
pip install git+https://github.com/saezlab/download-manager.git
```

If your are developing:
```bash
git clone https://github.com/saezlab/download-manager.git
cd download-manager
poetry install
```

## Usage

```python
import download_manager as dm

# Basic download to buffer
manager = dm.DownloadManager(backend='requests')
data = manager.download('https://www.google.com', dest=False)
print(data.read())

# Download to a file
manager = dm.DownloadManager(path='/tmp')
filepath = manager.download('https://www.google.com', dest='/tmp/google.html')
print(f"Downloaded to {filepath}")

# Download with cache integration
manager = dm.DownloadManager(path='/tmp')
filepath = manager.download('https://www.google.com')
print(f"Cached at {filepath}")
```

## API Overview

- `DownloadManager`: Main interface for downloads and cache management.
- `Descriptor`: Describes a download (URL, headers, POST/GET, etc).
- `CurlDownloader`: PyCurl-based downloader.
- `RequestsDownloader`: Requests-based downloader.

## Configuration

You can configure the download manager via keyword arguments or a config file:

```python
dm.DownloadManager(
    path='/my/cache/dir',
    backend='curl',  # or 'requests'
    # ...other options
)
```

## Development
- **Linting**: `poetry run flake8 download_manager`
- **Tests**: `poetry run pytest`
- **Coverage**: `poetry run pytest --cov`
- **Pre-commit**: Install with `pre-commit install`

## License

GNU General Public License v3.0

---

## Acknowledgements
Developed by the OmniPath team at Heidelberg University Hospital.

## Citation

If you use this software, please cite the repository and the OmniPath team.