import download_manager as dm


def test_filename(http_url):

    url = f'{http_url}/robots.txt'
    man = dm.DownloadManager()
    d = man._download(url, dest = False)

    assert d[2].filename == 'robots.txt'


