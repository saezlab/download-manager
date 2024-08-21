from __future__ import annotations

import cache_manager as cm


class DownloadManager:


    def __init__(self, path: str | None, pkg: str | None = None):

        self.cache = cm.Cache(path=path, pkg=pkg)
