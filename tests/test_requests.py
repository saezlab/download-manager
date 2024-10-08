import os
import json

import download_manager as dm

__all__ = [
    'test_most_simple',
    'test_post',
    'test_simple_tls',
    'test_simple_to_file',
]


def test_most_simple(http_url):

    dl = dm.RequestsDownloader(dm.Descriptor(http_url))
    dl.setup()
    dl.download()

    contents = dl._destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')


def test_simple_tls(https_url):

    dl = dm.RequestsDownloader(dm.Descriptor(https_url))
    dl.setup()
    dl.download()

    contents = dl._destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')


def test_simple_to_file(http_url, download_dir):

    path = os.path.join(download_dir, 'test.html')

    dl = dm.RequestsDownloader(dm.Descriptor(http_url), path)
    dl.setup()
    dl.download()

    assert os.path.exists(path)
    assert dl._destination.closed

    with open(path) as fp:
        contents = fp.read()

    assert contents.startswith('<!DOCTYPE html')


def test_post(http_url):

    http_url = f'{http_url}post'

    data = {'test_query': 'value', 'question': True, 'number': 2}

    dl = dm.RequestsDownloader(dm.Descriptor(http_url, query=data, post=True))
    dl.setup()
    dl.download()
    content = dl._destination.read()
    content = json.loads(content)

    data_str = {k: str(v) for k, v in data.items()}

    assert content["form"] == data_str


def test_json(http_url):

    http_url = f"{http_url}post"

    data = {"test_query": "value", "question": True, "number": 2}

    dl = dm.RequestsDownloader(dm.Descriptor(http_url, query=data, json=True))
    dl.setup()
    dl.download()
    content = dl._destination.read()
    content = json.loads(content)
    content["data"] = json.loads(content["data"])

    assert content["data"] == data


def test_multipart(http_url, download_dir):

    http_url = f'{http_url}post'
    test_file = os.path.join(download_dir, "tempfile.txt")

    with open(test_file, 'w') as fp:
        fp.write("Something")

    data = {'test_query': 'value', 'question': True, 'number': 2, 'file': test_file}

    dl = dm.RequestsDownloader(dm.Descriptor(http_url, multipart=data))
    dl.setup()
    dl.download()
    content = dl._destination.read()
    content = json.loads(content)
    data.pop('file')

    data_str = {k: str(v) for k, v in data.items()}

    assert content['form'] == data_str
    assert content['files'] == {'file': 'Something'}


def test_resp_headers(http_url, download_dir):

    query = {'resp_headers': 'resp_request'}
    resptest = dm.DownloadManager(download_dir)
    dltest = resptest._download(http_url, query=query)
    header_key = 'Content-Type'
    header_value = 'text/html; charset=utf-8'

    assert dltest[2] is not None

    header_dict = dltest[2].resp_headers

    assert len(header_dict) > 0
    assert header_key in header_dict
    assert header_value == header_dict[header_key]
