from __future__ import annotations

__all__ = [
    'DL_ATTRS',
    'DownloadManager',
]

import os
import datetime
import io

from pypath_common import data as _data
from cache_manager._status import Status
import cache_manager as cm
from . import _log, _downloader
from ._descriptor import Descriptor

DL_ATTRS = {
    'query',
    'post',
    'json',
    'multipart',
}


class DownloadManager:
    """
    Download manager, stores general configuration for the downloads and
    interfaces the downloads with the cache manager.
    """

    def __init__(
            self,
            path: str | None = None,
            pkg: str | None = None,
            config: str | dict | None = None,
            **kwargs,
    ):

        self._set_config(config, **kwargs)
        self._set_cache(path=path, pkg=pkg)


    def download(
            self,
            url: str,
            dest: str | bool | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            **kwargs,
    ) -> str | io.BytesIO | None:
        """
        Downloads a file from the given URL (if not already available in the
        cache).

        Args:
            url:
                URL address of the file to be downloaded/retrieved.
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

        desc = Descriptor(url, **kwargs)
        item = None
        downloader = None

        # XXX: Do we mean dest == True or bool(dest) is True?
        # Retrieve/create item from/in cache
        # If dest is anything but False
        if dest is True or dest is None:

            item = self._get_cache_item(desc, newer_than, older_than)
            dest = item.path

        # Perform the download
        if (
            # If there's an uninitialized item
            (item and item.rstatus == Status.UNINITIALIZED.value) or
            # Or no item and no existing file/dest is buffer
            (not item and (not os.path.exists(dest) or dest is False))
        ):

            if item:

                item.status = Status.WRITE.value

            backend = self.config.get('backend', 'requests').capitalize()
            downloader = getattr(_downloader, f'{backend}Downloader')(
                desc,
                dest or None,
            )

            downloader.download()

            if item:

                item.status = (
                    Status.READY.value
                        if downloader.ok else
                    Status.FAILED.value
                )

        # Return destination path/pointer
        if (
            # No donwload or successfully finished
            (downloader is None or downloader.ok) and
            # And file exists or dest is buffer
            (os.path.exists(dest) or dest is False) and
            # And no item or item is ready
            (not item or item.status == Status.READY.value)
        ):

            if dest is False: # File downloaded to buffer

                dest = downloader.destination

            return dest


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

            params = {desc[key] for key in DL_ATTRS if key in desc}

            item = self.cache.best_or_new(
                uri = desc['url'],
                params = params,
                older_than = older_than,
                newer_than = newer_than,
                new_status = Status.UNINITIALIZED.value,
                status = {Status.READY.value, Status.WRITE.value},
            )

            return item


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
