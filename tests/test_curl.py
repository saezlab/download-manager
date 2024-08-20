import os
import json

import download_manager as dm


def test_most_simple(http_url):

    dl = dm.CurlDownloader(dm.Descriptor(http_url))
    dl.setup()
    dl.download()

    contents = dl.destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')


def test_simple_tls(https_url):

    dl = dm.CurlDownloader(dm.Descriptor(https_url))
    dl.setup()
    dl.download()

    contents = dl.destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')


def test_simpl_to_file(http_url, download_dir):

    path = os.path.join(download_dir, 'test.html')

    dl = dm.CurlDownloader(dm.Descriptor(http_url), path)
    dl.setup()
    dl.download()

    assert os.path.exists(path)
    assert dl.destination.closed

    with open(path, 'r') as fp:
        contents = fp.read()

    assert contents.startswith('<!DOCTYPE html')


def test_post(http_url):

    http_url = f"{http_url}post"

    data = {"test_query": "value", "question": True, "number": 2}

    dl = dm.CurlDownloader(dm.Descriptor(http_url, query = data, post = True))
    dl.setup()
    dl.download()
    content = dl.destination.read()
    content = json.loads(content)

    assert content["form"] != data
