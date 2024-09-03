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

        self._set_config(config, kwargs)
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
        Args:
            dest: If `False`, goes to buffer.
        """

        desc = Descriptor(url, **kwargs)
        item = None
        downloader = None

        if dest is True or dest is None:

            item = self._get_cache_item(desc, newer_than, older_than)
            dest = item.path

        if (
            (item and item.rstatus == Status.UNINITIALIZED.value) or
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

        if (
            (downloader is None or downloader.ok) and
            (os.path.exists(dest) or dest is False) and
            (not item or item.status == Status.READY.value)
        ):

            if dest is False:

                dest = downloader.destination

            return dest


    def _get_cache_item(
            self,
            desc: Descriptor,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ) -> cm.CacheItem | None:

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
