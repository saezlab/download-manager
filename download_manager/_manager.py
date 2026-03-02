from __future__ import annotations

__all__ = [
    'DL_ATTRS',
    'DownloadManager',
]

import io
import os
import datetime
from pathlib import Path

from pypath_common import data as _data
from cache_manager._status import Status
import cache_manager as cm
import cache_manager.utils as cmutils

try:
    from cache_manager import _freshness as cm_freshness
except ImportError:
    cm_freshness = None
from . import _log, _downloader
from ._descriptor import Descriptor
from . import _constants

DL_ATTRS = {
    'query',
    'post',
    'json',
    'multipart',
    'headers',
}


class DownloadManager:
    """
    Download manager, stores general configuration for the downloads and
    interfaces the downloads with the cache manager.

    Args:
        path:
            Path where to set the cache directory.
        pkg:
            Package name. If no path is given, creates a directory with the
            given package name in the OS default cache directory. Optional,
            defaults to `None`.
        config:
            Accepts either a dictionary with the key/value pairs corresponding
            to parameter name/value or a path to the configuration file.
        **kwargs:
            Other/extra configuration parameters.

    Attrs:
        cache:
            Instance of `CacheManager` to interface with the cache.
        config:
            Configuration parameters for the download manager as dictionary of
            key/value pairs corresponding to the parameter name/value.
    """

    def __init__(
            self,
            path: str | None = None,
            pkg: str | None = None,
            config: str | dict | None = None,
            **kwargs,
    ):

        _log('Creating DownloadManager')
        self._set_config(config, **kwargs)
        self._set_cache(path=path, pkg=pkg)


    def download(
            self,
            url: str | Descriptor,
            dest: str | bool | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            check_freshness: bool = False,
            check_method: str = 'auto',
            force_download: bool = False,
            keep_old: bool = True,
            **kwargs,
    ) -> str | io.BytesIO | None:
        """
        Downloads a file from the given URL (if not already available in the
        cache).

        Args:
            url:
                URL address of the file to be downloaded/retrieved.
                Alternatively, a `Descriptor` object can be provided with all
                the download parameters.
            dest:
                Destination path, if set to `False`, the download is set to use
                the buffer (memory) for the download. If no destination is
                given, tries to obtain the destination path from the entry in
                the cache. Optional, defaults to `None`.
            newer_than:
                Only used when retrieving an item from the cache. Date of the
                item is required to be newer than. Optional, defaults to `None`.
            older_than:
                Only used when retrieving an item from the cache. Date of the
                item is required to be older than. Optional, defaults to `None`.
            **kwargs:
                Keyword arguments passed to the `Descriptor` instance. See the
                documentation of `Descriptor` for more details. Optional,
                defaults to `None`.

        Returns:
            The path where the requested file is located or the pointer to the
            file instance in the buffer.
        """

        *_, dest = self._download(
            url,
            dest=dest,
            newer_than=newer_than,
            older_than=older_than,
            check_freshness=check_freshness,
            check_method=check_method,
            force_download=force_download,
            keep_old=keep_old,
            **kwargs
        )

        return dest


    def _download(
            self,
            url: str | Descriptor,
            dest: str | bool | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            check_freshness: bool = False,
            check_method: str = 'auto',
            force_download: bool = False,
            keep_old: bool = True,
            retries: int | None = None,
            **kwargs,
    ) -> tuple[Descriptor, cm.CacheItem, str | io.BytesIO | None]:
        """
        Downloads a file from the given URL (if not already available in the
        cache). Secret method called by the public one but returning also the
        instances of the descriptor and the cache item alongside with the
        destination.

        Args:
            url:
                URL address of the file to be downloaded/retrieved.
                Alternatively, a `Descriptor` object can be provided with all
                the download parameters.
            dest:
                Destination path, if set to `False`, the download is set to use
                the buffer (memory) for the download. If no destination is
                given, tries to obtain the destination path from the entry in
                the cache. Optional, defaults to `None`.
            newer_than:
                Only used when retrieving an item from the cache. Date of the
                item is required to be newer than. Optional, defaults to `None`.
            older_than:
                Only used when retrieving an item from the cache. Date of the
                item is required to be older than. Optional, defaults to `None`.
            **kwargs:
                Keyword arguments passed to the `Descriptor` instance. See the
                documentation of `Descriptor` for more details. Optional,
                defaults to `None`.

        Returns:
            Tuple of three elements in the following order: The instance of the
            download `Descriptor`, the instance of the corresponding `CacheItem`
            if there's a cache available and the path where the requested file
            is located or the pointer to the file instance in the buffer.
        """

        _log('Starting the download')
        desc = (
            url
                if isinstance(url, Descriptor) else
            Descriptor(url, **kwargs)
        )
        backend = self.config.get('backend', 'requests').capitalize()
        downloader_cls = getattr(_downloader, f'{backend}Downloader')

        _log(f'Using backend: {backend}')

        show_progress = self.config.get('progress', True)

        item = None
        downloader = None

        # Deciding what to do:
        # 1) If dest is a path -> Download
        # 2) If dest is True or None -> attempt to get from cache ->
        #   2.1) If exists in cache (and disk) do not download
        #   2.2) If no cache available, go to buffer
        # 3) If dest is False -> download to buffer
        path = None
        to_buffer = False
        cache = False

        # Case 1)
        if isinstance(dest, str):

            path = dest
            # to_buffer = False, keeps default

            _log(f'Downloading to path: {path}')

        # Case 2)
        elif dest is True or dest is None:

            cache = self.cache is not None
            to_buffer = not cache

            _log(f'Cache is available {cache}')

        # Case 3)
        elif dest is False:

            to_buffer = True

        _log(f'Downloading to buffer {to_buffer}')

        for i in range(retries or 1):
            _log(f'Attempt number {i}')

            if cache:

                item = self._get_cache_item(desc, newer_than, older_than)
                path = item.path
                _log(f'Cache path: {path}')

            # Use existing local file when possible
            if (
                path and
                os.path.exists(path) and
                not force_download and
                (
                    item is None or
                    item.rstatus != Status.UNINITIALIZED.value
                )
            ):

                _log(f'Local file exists: {path}')

                if not check_freshness:

                    _log('Using existing local file from cache')
                    break

                if cm_freshness is None:

                    _log(
                        'Freshness check requested but cache_manager '
                        'freshness module is unavailable; using cached file'
                    )
                    break

                _log('Checking if remote version is newer')
                remote_headers = cm_freshness.get_remote_headers(
                    desc['url'],
                    timeout=self.config.get('timeout', 30),
                )

                if remote_headers:

                    local_metadata = cm_freshness.metadata_from_item(item)
                    is_current, reason = cm_freshness.check_freshness(
                        path,
                        remote_headers,
                        local_metadata,
                        check_method,
                    )
                    _log(
                        f'Freshness check result: {is_current}, '
                        f'reason: {reason}'
                    )

                    if is_current:

                        _log('Local file is current, using cached version')
                        break

                    _log('Remote version is newer, downloading')

                    if keep_old:

                        timestamp = datetime.datetime.now().strftime(
                            '%Y%m%d_%H%M%S'
                        )
                        existing_path = Path(path)
                        old_path = existing_path.parent / (
                            f'{existing_path.stem}_{timestamp}'
                            f'{existing_path.suffix}'
                        )
                        existing_path.rename(old_path)
                        _log(f'Renamed old file to: {old_path}')

                    if item is not None:

                        item.status = Status.UNINITIALIZED.value

                else:

                    _log(
                        'Could not get remote headers for freshness check; '
                        'redownloading'
                    )

                    if item is not None:

                        item.status = Status.UNINITIALIZED.value

            # Instantiate the downloader (no download yet)
            downloader = downloader_cls(desc, path, progress=show_progress)

            # Perform the download or break the loop when ok or already in cache
            if (
                force_download or
                not item or
                item.rstatus == Status.UNINITIALIZED.value
            ):
                _log(f'No valid version in cache, starting download')

                self._report_started(item)
                downloader.download()
                self._report_finished(item, downloader, desc)

                if downloader.ok:

                    _log(f'Download was successful')
                    break

            else:
                _log(f'Item retrieved from cache: {path}')
                break

        _log('Finished the download')

        return (
            desc,
            item,
            downloader,
            downloader._destination if to_buffer else path
        )


    def _get_cache_item(
            self,
            desc: Descriptor,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ) -> cm.CacheItem | None:
        """
        Retrieves a cache item instance from the cache if it exists, otherwise
        creates a new one based on the given `Descriptor`.

        Args:
            desc:
                Instance of `Descriptor` containing the information of the item
                to be retrieved or created.
            newer_than:
                Date of the item is required to be newer than. Optional,
                defaults to `None`.
            older_than:
                Date of the item is required to be older than. Optional,
                defaults to `None`.

        Returns:
            The `CacheItem` instance of the retrieved/created entry from the
            cache.
        """

        if self.cache is not None:

            dl_params = {key: desc[key] for key in DL_ATTRS if key in desc}
            desc_params = dict(desc)
            _log(f'DL_PARAMS: {cmutils.serialize(dl_params)}')
            _log(f'DESC_PARAMS: {cmutils.serialize(desc_params)}')

            item = self.cache.best_or_new(
                uri = desc['baseurl'],
                params = {_constants.DL_PARAMS_KEY: dl_params},
                attrs = {_constants.DL_DESC_KEY: desc_params},
                older_than = older_than,
                newer_than = newer_than,
                new_status = Status.UNINITIALIZED.value,
                status = {Status.READY.value, Status.WRITE.value},
            )

            _log(f'Cache item: {item.__repr__()}')

            return item


    def _report_finished(
        self,
        item: cm.CacheItem,
        downloader: cm.Downloader.AbstractDownloader,
        desc: Descriptor,
    ):
        """
        Updates the cache entry relevant entries when a download has
        successfully been performed.

        Args:
            item:
                The instance of the cache item to report as finished.
            downloader:
                The instance of the downloader.
        """

        if item is not None:

            item.status = (
                Status.READY.value
                    if downloader.ok else
                Status.FAILED.value
            )
            item.update_date('download_finished')
            item.accessed()
            item.update_date()

            method = 'POST' if desc.get('post') else 'GET'
            headers = downloader.resp_headers or {}

            args = {
                'attrs': {
                    'resp_headers': headers,
                    'url': desc['url'],
                    'download_method': method,
                },
            }

            if downloader.filename:

                args['file_name'] = downloader.filename

                if ext := downloader.ext:

                    args['ext'] = ext

            args['attrs']['sha256'] = downloader.sha256
            args['attrs']['size'] = downloader.size
            args['attrs']['http_code'] = downloader.http_code

            if method == 'POST':
                args['attrs']['post_data'] = desc.get('query')
            else:
                args['attrs']['query_params'] = desc.get('query')

            if etag := headers.get('ETag') or headers.get('etag'):
                args['attrs']['etag'] = etag

            if (
                last_modified :=
                headers.get('Last-Modified') or headers.get('last-modified')
            ):
                args['attrs']['last_modified'] = last_modified

            _log(
                f'Saving download metadata to cache. '
                f'Size = {downloader.size}, HTTP code = {downloader.http_code}'
            )

            item.update(**args)


    def _report_started(self, item: cm.CacheItem | None):
        """

        """

        if item:

            item.status = Status.WRITE.value
            item.update_date('download_started')
            item.update_date()


    def _set_cache(self, path: str | None, pkg: str | None = None):
        """
        Initializes the cache manager interface if a path or package name given.

        Args:
            path:
                Path where to set the cache directory.
            pkg:
                Package name. If no path is given, creates a directory with the
                given package name in the OS default cache directory. Optional,
                defaults to `None`.
        """

        path = path or self.config.get('path', None)
        pkg = pkg or self.config.get('pkg', None)

        if path or pkg:

            self.cache = cm.Cache(path=path, pkg=pkg)

        else:

            self.cache = None


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
