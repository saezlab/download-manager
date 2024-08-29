from __future__ import annotations

import os
import datetime

import cache_manager as cm
from cache_manager._status import Status

from pypath_common import data as _data
from . import _downloader
from ._descriptor import Descriptor
from . import _log


__all__ = [
    'DownloadManager',
]

DL_ATTRS = {
    'query',
    'post',
    'json',
    'multipart',
}


class DownloadManager:


    def __init__(
            self,
            path: str | None,
            pkg: str | None = None,
            config: str | dict | None = None,
            **kwargs,
    ):

        self._set_config(config, kwargs)
        self._set_cache(path=path, pkg=pkg)


    def _set_config(self, config: str | dict | None, kwargs):

        if isinstance(config, str) and os.path.exists(config):

            config = _data.load(config)

        config = config or {}
        config.update(kwargs)
        self.config = config


    def _set_cache(self, path: str | None, pkg: str | None = None):

        path = path or self.config.get('path', None)
        pkg = pkg or self.config.get('pkg', None)

        if path or pkg:

            self.cache = cm.Cache(path=path, pkg=pkg)


    def download(
            self,
            url: str,
            dest: str | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            **kwargs
        ) -> str:

        desc = Descriptor(url, **kwargs)
        item = None
        dwnldr = True

        if not dest:
            item = self._get_cache_item(desc, newer_than, older_than)
            dest = item.path

        if (
            (item and item.rstatus == Status.UNINITIALIZED.value) or
            (not item and not os.path.exists(dest))
        ):

            if item:

                item.status = Status.WRITE.value

            backend = self.config.get('backend', 'requests').capitalize()
            dwnldr = getattr(_downloader, f'{backend}Downloader')(desc, dest)

            dwnldr.download()

            if item:

                item.status = Status.READY.value if dwnldr.ok else Status.FAILED.value

        if (
            dwnldr.ok and
            os.path.exists(dest) and
            (not item or item.status == Status.READY.value)
        ):

            _log('Download successful.')

        return dest


    def _get_cache_item(
            self,
            desc: Descriptor,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
        ) -> cm.CacheItem | None:

        if self.cache:

            param = {desc[key] for key in DL_ATTRS if key in desc}

            item = self.cache.best_or_new(
                uri = desc.url,
                param = param,
                older_than = older_than,
                newer_than = newer_than,
                new_status = Status.UNINITIALIZED.value,
                status = {Status.READY.value, Status.WRITE.value},
            )

            return item
