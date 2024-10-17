import os
import io
import download_manager as dm


def test_filename(http_url):

    url = f'{http_url}/robots.txt'
    man = dm.DownloadManager()
    d = man._download(url, dest = False)

    assert d[2].filename == 'robots.txt'


def test_filename_contdispos_buffer(http_url):

    url = (
        f'{http_url}/response-headers?'
        'Content-Type=text/plain;%20charset=UTF-8&'
        'Content-Disposition=attachment;%20filename%3d%22test.json%22'
    )

    man = dm.DownloadManager()
    d = man._download(url, dest = False)

    assert d[2].filename == 'test.json'
    assert isinstance(d[3], io.BytesIO)


def test_filename_contdispos_disk(http_url, download_dir):

    url = (
        f'{http_url}/response-headers?'
        'Content-Type=text/plain;%20charset=UTF-8&'
        'Content-Disposition=attachment;%20filename%3d%22test.json%22'
    )

    man = dm.DownloadManager(path = download_dir)
    d = man._download(url)

    assert d[2].filename == 'test.json'
    assert os.path.exists(d[3])
    #assert d[3].endswith('.json') # TODO: should update filename in cache after download


def test_size(http_url):

    url = f'{http_url}/robots.txt?foobar=hello'
    man = dm.DownloadManager()
    d = man._download(url, dest = False)

    assert d[2].size > 0
