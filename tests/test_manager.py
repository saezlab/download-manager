import io

import download_manager as dm

__all__ = []


def test_dest_write_buffer(http_url):
    manager = dm.DownloadManager()
    dest = manager.download(http_url, dest=False)

    assert manager.cache is None
    assert isinstance(dest, io.BytesIO)
    assert dest.read().startswith(b'<!DOCTYPE html')