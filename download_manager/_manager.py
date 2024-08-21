from __future__ import annotations

import os

import cache_manager as cm

from pypath_common import data as _data
from . import _downloader
from ._descriptor import Descriptor

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


    def download(self, url: str, dest: str | None = None, **kwargs) -> str:

        desc = Descriptor(url, **kwargs)
        dest = dest or self._cache_path(desc)

        if not dest:
            # temporary:
            raise ValueError('Can not download without a destination path.')

        backend = self.config.get('backend', 'requests').capitalize()
        downloader = getattr(_downloader, f'{backend}Downloader')(desc, dest)

        downloader.download()

        return dest


    def _cache_path(self, desc: Descriptor) -> str | None:

        if self.cache:

            param = {desc[key] for key in DL_ATTRS if key in desc}

            item = self.cache.best_or_new(desc.url, param)

            return item.path
