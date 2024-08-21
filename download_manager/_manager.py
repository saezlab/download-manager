from __future__ import annotations

import cache_manager as cm


class DownloadManager:


    def __init__(self, cache_dir: str | None = None):

        self.cache = cm.Cache(cache_dir)
