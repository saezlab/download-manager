from __future__ import annotations

__all__ = [
    'DL_ATTRS',
    'DownloadManager',
]

import io
import os
import datetime
import logging
from pathlib import Path

from pypath_common import data as _data
from cache_manager._status import Status
import cache_manager as cm
import cache_manager.utils as cmutils

try:
    from cache_manager import _freshness as cm_freshness
except ImportError:
    cm_freshness = None
from . import (
    _downloader,
)
from ._descriptor import Descriptor
from . import _constants

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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

        logger.debug(
            'Initializing DownloadManager path=%r pkg=%r config_type=%s',
            path,
            pkg,
            type(config).__name__,
        )
        self._set_config(config, **kwargs)
        self._set_cache(path=path, pkg=pkg)
        logger.info(
            'DownloadManager initialized cache_enabled=%s backend=%s',
            self.cache is not None,
            self.config.get('backend', 'requests'),
        )


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

        logger.debug(
            'download called url_type=%s dest=%r kwargs_keys=%s',
            type(url).__name__,
            dest,
            sorted(kwargs.keys()),
        )
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

        logger.info(
            'Starting internal download flow retries=%s dest=%r',
            retries or 1,
            dest,
        )
        desc = (
            url
                if isinstance(url, Descriptor) else
            Descriptor(url, **kwargs)
        )
        backend = self.config.get('backend', 'requests').capitalize()
        logger.debug('Resolved backend class prefix: %s', backend)
        downloader_cls = getattr(_downloader, f'{backend}Downloader')

        logger.debug(f'Using backend: {backend}')

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
            logger.info('Destination is explicit path: %s', dest)

            path = dest
            # to_buffer = False, keeps default

            logger.debug(f'Downloading to path: {path}')

        # Case 2)
        elif dest is True or dest is None:
            logger.debug(
                'Destination uses auto mode: dest=%r cache_available=%s',
                dest,
                self.cache is not None,
            )

            cache = self.cache is not None
            to_buffer = not cache

            logger.debug(f'Cache is available {cache}')

        # Case 3)
        elif dest is False:
            logger.info('Destination forced to memory buffer')

            to_buffer = True

        logger.debug(f'Downloading to buffer {to_buffer}')

        for i in range(retries or 1):
            logger.info('Download attempt %d/%d', i + 1, retries or 1)

            if cache:

                item = self._get_cache_item(desc, newer_than, older_than)
                path = item.path
                logger.debug(
                    'Cache returned item key=%s status=%s path=%s',
                    getattr(item, 'key', None),
                    getattr(item, 'rstatus', None),
                    path,
                )

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


                logger.debug(f'Local file exists: {path}')

                if not check_freshness:

                    logger.info('Using existing local file from cache')
                    break

                if cm_freshness is None:

                    logger.warning(
                        'Freshness check requested but cache_manager '
                        'freshness module is unavailable; using cached file'
                    )
                    break


                logger.debug('Checking if remote version is newer')
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

                    logger.debug(
                        f'Freshness check result: {is_current}, '
                        f'reason: {reason}'
                    )

                    if is_current:


                        logger.info('Local file is current, using cached version')
                        break


                    logger.info('Remote version is newer, downloading')

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

                        logger.info(f'Renamed old file to: {old_path}')

                    if item is not None:

                        item.status = Status.UNINITIALIZED.value

                else:

                    logger.warning(
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

                logger.info('No valid version in cache, starting download')

                self._report_started(item)
                downloader.download()
                self._report_finished(item, downloader, desc)

                if downloader.ok:

                    logger.info(
                        'Download succeeded http_code=%s path=%r to_buffer=%s',
                        downloader.http_code,
                        path,
                        to_buffer,
                    )
                    break
                logger.warning(
                    'Download attempt %d completed but not successful http_code=%s',
                    i + 1,
                    downloader.http_code,
                )

            else:

                logger.info(f'Item retrieved from cache: {path}')
                break


        logger.info('Finished the download')
        if downloader and not downloader.ok:
            logger.error(
                'All download attempts exhausted or failed http_code=%s',
                downloader.http_code,
            )

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
            logger.debug('Querying cache for descriptor baseurl=%s', desc['baseurl'])

            dl_params = {key: desc[key] for key in DL_ATTRS if key in desc}
            desc_params = dict(desc)

            logger.debug(f'DL_PARAMS: {cmutils.serialize(dl_params)}')
            logger.debug(f'DESC_PARAMS: {cmutils.serialize(desc_params)}')

            item = self.cache.best_or_new(
                uri = desc['baseurl'],
                params = {_constants.DL_PARAMS_KEY: dl_params},
                attrs = {_constants.DL_DESC_KEY: desc_params},
                older_than = older_than,
                newer_than = newer_than,
                new_status = Status.UNINITIALIZED.value,
                status = {Status.READY.value, Status.WRITE.value},
            )

            logger.info(
                'Cache item selected key=%s status=%s',
                getattr(item, 'key', None),
                getattr(item, 'rstatus', None),
            )

            return item
        logger.debug('Cache not configured; skipping cache lookup')


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
            logger.debug(
                'Reporting finished download for cache item key=%s',
                getattr(item, 'key', None),
            )

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


            logger.debug(
                f'Saving download metadata to cache. '
                f'Size = {downloader.size}, HTTP code = {downloader.http_code}'
            )

            item.update(**args)
            logger.info(
                'Cache metadata updated key=%s status=%s http_code=%s',
                getattr(item, 'key', None),
                item.status,
                downloader.http_code,
            )
        else:
            logger.debug('No cache item to update in _report_finished')


    def _report_started(self, item: cm.CacheItem | None):
        """

        """

        if item:
            logger.debug(
                'Reporting started download for cache item key=%s',
                getattr(item, 'key', None),
            )

            item.status = Status.WRITE.value
            item.update_date('download_started')
            item.update_date()
        else:
            logger.debug('No cache item to mark started')


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
            logger.info('Initializing cache manager path=%r pkg=%r', path, pkg)

            self.cache = cm.Cache(path=path, pkg=pkg)

        else:
            logger.info('Cache manager disabled (no path/pkg provided)')

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
            logger.info('Loading manager config file from %s', config)

            config = _data.load(config)
        elif isinstance(config, str):
            logger.warning('Config path does not exist: %s', config)

        config = config or {}
        config.update(kwargs)
        self.config = config
        logger.debug('Manager config keys: %s', sorted(self.config.keys()))
