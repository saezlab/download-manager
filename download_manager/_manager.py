from __future__ import annotations

import cache_manager as cm
from pypath_common import data as _data


class DownloadManager:


    def __init__(
            self,
            path: str | None,
            pkg: str | None = None,
            config: str | dict | None = None,
            **kwargs
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
