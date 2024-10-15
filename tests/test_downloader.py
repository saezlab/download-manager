import download_manager as dm


def test_filename(http_url):

    url = f'{http_url}/robots.txt'
    man = dm.DownloadManager()
    d = man._download(url, dest = False)

    assert d[2].filename == 'robots.txt'


def test_filename_contdispos(http_url):

    url = (
        f'{http_url}/response-headers?'
        'Content-Type=text/plain;%20charset=UTF-8&'
        'Content-Disposition=attachment;%20filename%3d%22test.json%22'
    )

    man = dm.DownloadManager()
    d = man._download(url, dest = False)

    assert d[2].filename == 'test.json'


def test_filename_contdispos(http_url):

    url = (
        f'{http_url}/response-headers?'
        'Content-Type=text/plain;%20charset=UTF-8&'
        'Content-Disposition=attachment;%20filename%3d%22test.json%22'
    )

    man = dm.DownloadManager()
    d = man._download(url)

    assert d[2].filename == 'test.json'
