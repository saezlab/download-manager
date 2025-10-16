from __future__ import annotations

__all__ = [
    'DownloadManager',
]

import io
import os
from pathlib import Path
from datetime import datetime

from pypath_common import data as _data
from . import _log, _downloader, _freshness
from ._descriptor import Descriptor
from ._storage import MetadataStore


class DownloadManager:
    """
    Simple file-based download manager.

    Args:
        data_folder:
            Base folder for all raw data files.
        module_name:
            Subfolder name for this module (e.g., 'uniprot', 'ensembl').
        config:
            Accepts either a dictionary with the key/value pairs corresponding
            to parameter name/value or a path to the configuration file.
        **kwargs:
            Other/extra configuration parameters.

    Attrs:
        data_folder:
            Base data folder path.
        module_name:
            Module subfolder name.
        module_folder:
            Full path to module subfolder.
        metadata:
            MetadataStore instance for managing file metadata.
        config:
            Configuration parameters for the download manager as dictionary of
            key/value pairs corresponding to the parameter name/value.
    """

    def __init__(
            self,
            data_folder: str | Path,
            module_name: str,
            config: str | dict | None = None,
            **kwargs,
    ):
        self._set_config(config, **kwargs)
        self.data_folder = Path(data_folder)
        self.module_name = module_name
        self.module_folder = self.data_folder / module_name
        self.module_folder.mkdir(parents=True, exist_ok=True)

        self.metadata = MetadataStore(data_folder, module_name)


    def download(
            self,
            url: str | Descriptor,
            filename: str | None = None,
            dest: str | bool | None = None,
            check_freshness: bool = False,
            check_method: str = 'auto',
            force_download: bool = False,
            keep_old: bool = True,
            **kwargs,
    ) -> str | io.BytesIO | Path | None:
        """
        Downloads a file from the given URL.

        Args:
            url:
                URL address of the file to be downloaded/retrieved.
                Alternatively, a `Descriptor` object can be provided with all
                the download parameters.
            filename:
                Name for the downloaded file. If None, auto-detected from URL
                or Content-Disposition header.
            dest:
                Destination path. If set to `False`, downloads to buffer (memory).
                If None, uses module_folder/{filename}. If string, uses that path.
            check_freshness:
                If True and local file exists, check if remote version is newer.
            check_method:
                Method for freshness check: 'auto', 'etag', 'modified', 'hash', 'size'.
            force_download:
                If True, always download regardless of local file existence.
            keep_old:
                If True and downloading a newer version, keep old file with
                timestamp suffix.
            **kwargs:
                Keyword arguments passed to the `Descriptor` instance.

        Returns:
            Path to the downloaded file, or BytesIO buffer if dest=False.
        """

        _log('Starting the download')

        # Create descriptor
        desc = (
            url
                if isinstance(url, Descriptor) else
            Descriptor(url, **kwargs)
        )

        # Select backend
        backend = self.config.get('backend', 'requests').capitalize()
        downloader_cls = getattr(_downloader, f'{backend}Downloader')
        _log(f'Using backend: {backend}')

        # Determine destination
        to_buffer = dest is False
        needs_download = force_download

        if to_buffer:
            # Download to buffer
            _log('Downloading to buffer')
            downloader = downloader_cls(desc, None)
            downloader.download()
            return downloader._destination if downloader.ok else None

        # Determine filename if not provided
        if not filename and isinstance(dest, str):
            filename = os.path.basename(dest)
        elif not filename:
            # Will be determined from URL or headers after download
            filename = None

        # Determine file path
        if isinstance(dest, str):
            file_path = Path(dest)
        elif filename:
            file_path = self.module_folder / filename
        else:
            # We'll determine it after getting headers
            file_path = None

        # Check if local file exists and if we should check freshness
        if file_path and file_path.exists() and not force_download:
            _log(f'Local file exists: {file_path}')

            if check_freshness:
                _log('Checking if remote version is newer')
                remote_headers = _freshness.get_remote_headers(
                    desc['url'],
                    timeout=self.config.get('timeout', 30),
                )

                if remote_headers:
                    local_metadata = self.metadata.load(file_path.name)
                    is_current, reason = _freshness.check_freshness(
                        file_path,
                        remote_headers,
                        local_metadata,
                        check_method,
                    )

                    _log(f'Freshness check result: {is_current}, reason: {reason}')

                    if is_current:
                        _log('Local file is current, using cached version')
                        return file_path
                    else:
                        _log('Remote version is newer, downloading')
                        needs_download = True

                        if keep_old:
                            # Rename old file with timestamp
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            old_path = file_path.parent / f"{file_path.stem}_{timestamp}{file_path.suffix}"
                            file_path.rename(old_path)
                            _log(f'Renamed old file to: {old_path}')
                else:
                    _log('Could not get remote headers for freshness check')
            else:
                # File exists and no freshness check requested
                _log('Using existing local file')
                return file_path

        # Download the file
        _log(f'Starting download from {desc["url"]}')

        # First, we might need to do a preliminary download to get the filename
        if not file_path:
            # Download to buffer first to get headers
            temp_downloader = downloader_cls(desc, None)
            temp_downloader.setup()

            if hasattr(temp_downloader, 'handler'):
                # For curl, we can get headers without full download
                # For now, let's use the URL-based filename
                filename = temp_downloader.filename or 'download'
                file_path = self.module_folder / filename
            else:
                # For requests, similar approach
                filename = temp_downloader.filename or 'download'
                file_path = self.module_folder / filename

        # Perform the actual download
        downloader = downloader_cls(desc, str(file_path))
        downloader.download()

        if downloader.ok:
            _log(f'Download successful: {file_path}')

            # Save metadata with download details
            self.metadata.save_from_headers(
                file_path.name,
                downloader.resp_headers,
                sha256=downloader.sha256,
                size=downloader.size,
                url=desc['url'],
                method='POST' if desc.get('post') else 'GET',
                post_data=desc.get('query') if desc.get('post') else None,
                query_params=desc.get('query') if not desc.get('post') else None,
            )

            return file_path
        else:
            _log(f'Download failed with HTTP code: {downloader.http_code}')
            return None


    def _set_config(self, config: str | dict | None, **kwargs):
        """
        Establishes the configuration for the download manager.

        Args:
            config:
                Accepts either a dictionary with the key/value pairs
                corresponding to parameter name/value or a path to the
                configuration file.
            **kwargs:
                Other/extra configuration parameters.
        """

        if isinstance(config, str) and os.path.exists(config):
            config = _data.load(config)

        config = config or {}
        config.update(kwargs)
        self.config = config
