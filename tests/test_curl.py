import download_manager as dm


def test_most_simple(http_url):

    dl = dm.CurlDownloader(dm.Descriptor(http_url))
    dl.setup()
    dl.download()

    contents = dl.destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')
