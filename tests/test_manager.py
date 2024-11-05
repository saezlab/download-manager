import io
import os
from datetime import datetime, timedelta, timezone
import dateutil

import cache_manager as cm
from cache_manager._status import Status
from cache_manager import utils

import download_manager as dm
from download_manager import _constants

__all__ = []


def test_dest_buffer(http_url, d_config):

    manager = dm.DownloadManager(**d_config)
    dest = manager.download(http_url, dest=False)

    assert manager.cache is None
    assert isinstance(dest, io.BytesIO)
    assert dest.read().startswith(b'<!DOCTYPE html')


def test_dest_path(http_url, download_dir, d_config):

    path = os.path.join(download_dir, 'test_dest_path.html')
    manager = dm.DownloadManager(**d_config)
    dest = manager.download(http_url, path)

    assert manager.cache is None
    assert isinstance(dest, str)
    assert dest == path
    assert os.path.exists(dest)

    with open(dest) as fp:

        assert fp.read().startswith('<!DOCTYPE html')


def test_dest_cache(http_url, download_dir, d_config):

    manager = dm.DownloadManager(path= download_dir, **d_config)
    dest = manager.download(http_url)

    assert isinstance(manager.cache, cm.Cache)
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    with open(dest) as fp:

        assert fp.read().startswith('<!DOCTYPE html')


def test_cache_integration(http_url, download_dir, d_config):

    query = {'foo': 'bar'}
    manager = dm.DownloadManager(path=download_dir, **d_config)
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


def test_cache_desc_reconstitution(http_url, download_dir, d_config):

    query = {'foo2': 'bar'}
    manager = dm.DownloadManager(path=download_dir, **d_config)
    desc, item, *_ = manager._download(
        http_url,
        query=query,
        timeout=5,
    )

    assert 'timeout' in item.attrs[_constants.DL_DESC_KEY]

    desc_recon = dm.Descriptor(**item.attrs[_constants.DL_DESC_KEY])

    assert dict(desc_recon) == dict(desc)

    desc_recon2, *_ = manager._download(desc_recon)

    assert dict(desc_recon2) == dict(desc)


def test_timestamps(http_url, download_dir, d_config):

    query = {'updatefoo': 'updatebar'}
    manager = dm.DownloadManager(path=download_dir, **d_config)
    desc, item, *_ = manager._download(
        http_url,
        query=query,
        timeout=5,
    )

    item = item._from_main()

    assert datetime.now() - dateutil.parser.parse(item.date) < timedelta(seconds=10)
    assert datetime.now() - dateutil.parser.parse(item.attrs['download_started']) < timedelta(seconds=10)
    assert datetime.now() - dateutil.parser.parse(item.attrs['download_finished']) < timedelta(seconds=10)
    assert datetime.now() - dateutil.parser.parse(item.last_read) < timedelta(seconds=10)
    assert item.status == cm._status.Status.READY.value


def test_store_req_headers(http_url, download_dir, d_config):

    query = {'reqheaders': 'test'}
    headers = ['X-Test: test']
    manager = dm.DownloadManager(path=download_dir, **d_config)
    _ = manager._download(
        http_url,
        query=query,
        headers=headers,
    )

    item = manager.cache.best_or_new(
        http_url,
        params = {'query': query, 'headers': headers},
    )

    assert item.params['headers'] == headers


def test_store_resp_header(http_url, download_dir, d_config):

    query = {'resp_headers': 'test'}
    manager = dm.DownloadManager(path=download_dir, **d_config)
    _ = manager._download(
        http_url,
        query=query,
    )

    item = manager.cache.best_or_new(
        http_url,
        params={'dl_params': {'query': query, 'headers': []}},
    )

    assert 'Content-Type' in item.attrs['resp_headers']
    assert (
        item.attrs['resp_headers']['Content-Type'] ==
        {'0': 'text/html', 'charset': 'utf-8'}
    )


def test_retries(http_url, download_dir, d_config):

    url = http_url + 'status/500'
    manager = dm.DownloadManager(path=download_dir, **d_config)
    dl = manager._download(dm.Descriptor(url), retries=3)

    key = dl[1].key

    cacheitem = manager.cache.search(key=key)

    assert len(cacheitem) == 3
    assert all(item._status == Status.FAILED.value for item in cacheitem)
    assert all(item.attrs['http_code'] == 500 for item in cacheitem)
