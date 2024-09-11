import io
import os

import cache_manager as cm

import download_manager as dm
from download_manager import _constants

__all__ = []


def test_dest_buffer(http_url):

    manager = dm.DownloadManager()
    dest = manager.download(http_url, dest=False)

    assert manager.cache is None
    assert isinstance(dest, io.BytesIO)
    assert dest.read().startswith(b'<!DOCTYPE html')


def test_dest_path(http_url, download_dir):

    path = os.path.join(download_dir, 'test_dest_path.html')
    manager = dm.DownloadManager()
    dest = manager.download(http_url, path)

    assert manager.cache is None
    assert isinstance(dest, str)
    assert dest == path
    assert os.path.exists(dest)

    with open(dest) as fp:

        assert fp.read().startswith('<!DOCTYPE html')


def test_dest_cache(http_url, download_dir):

    manager = dm.DownloadManager(path = download_dir)
    dest = manager.download(http_url)

    assert isinstance(manager.cache, cm.Cache)
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    with open(dest) as fp:

        assert fp.read().startswith('<!DOCTYPE html')


def test_cache_integration(http_url, download_dir):

    query = {'foo': 'bar'}
    manager = dm.DownloadManager(path=download_dir)
    dest = manager.download(
        http_url,
        query=query,
    )

    it = manager.cache.best_or_new(
        http_url,
        params = {
            _constants.DL_PARAMS_KEY: {'query': query},
        }
    )

    # Checking attrs
    assert it.params['_uri'] == http_url
    assert it.params[_constants.DL_PARAMS_KEY]['query'] == query
    assert it.uri == http_url


def test_cache_desc_reconstitution(http_url, download_dir):

    query = {'foo2': 'bar'}
    manager = dm.DownloadManager(path=download_dir)
    desc, item, dest = manager._download(
        http_url,
        query=query,
        timeout=5,
    )

    assert 'timeout' in item.attrs[_constants.DL_DESC_KEY]

    desc_recon = dm.Descriptor(**item.attrs[_constants.DL_DESC_KEY])

    # how destination is written into the database???
    assert dict(desc_recon) == dict(desc)

    it = manager.cache.best_or_new(
        http_url,
        params = {
            _constants.DL_PARAMS_KEY: {'query': query},
        }
    )
