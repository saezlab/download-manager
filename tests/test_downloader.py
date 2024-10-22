import pytest

import os
import io
import json

import requests

import download_manager as dm


def test_most_simple(http_url, downloader):

    dl = downloader(dm.Descriptor(http_url))
    dl.setup()
    dl.download()

    contents = dl._destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')


def test_simple_tls(https_url, downloader):

    dl = downloader(dm.Descriptor(https_url))
    dl.setup()
    dl.download()

    contents = dl._destination.read().decode('utf-8')

    assert len(contents)
    assert contents.startswith('<!DOCTYPE html')


def test_simple_to_file(http_url, download_dir, downloader):

    path = os.path.join(download_dir, 'test.html')

    dl = downloader(dm.Descriptor(http_url), path)
    dl.setup()
    dl.download()

    assert os.path.exists(path)
    assert dl._destination.closed

    with open(path) as fp:
        contents = fp.read()

    assert contents.startswith('<!DOCTYPE html')


def test_post(http_url, downloader):

    http_url = f'{http_url}post'

    data = {'test_query': 'value', 'question': True, 'number': 2}

    dl = downloader(dm.Descriptor(http_url, query=data, post=True))
    dl.setup()
    dl.download()
    content = dl._destination.read()
    content = json.loads(content)

    data_str = {k: str(v) for k, v in data.items()}

    assert content["form"] == data_str


def test_json(http_url, downloader):

    http_url = f"{http_url}post"

    data = {"test_query": "value", "question": True, "number": 2}

    dl = downloader(dm.Descriptor(http_url, query = data, json = True))
    dl.setup()
    dl.download()
    content = dl._destination.read()
    content = json.loads(content)
    content["data"] = json.loads(content["data"])

    assert content["data"] == data


def test_multipart(http_url, download_dir, downloader):

    http_url = f'{http_url}post'
    test_file = os.path.join(download_dir, "tempfile.txt")

    with open(test_file, 'w') as fp:
        fp.write("Something")

    data = {'test_query': 'value', 'question': True, 'number': 2, 'file': test_file}

    dl = downloader(dm.Descriptor(http_url, multipart=data))
    dl.setup()
    dl.download()
    content = dl._destination.read()
    content = json.loads(content)
    data.pop('file')

    data_str = {k: str(v) for k, v in data.items()}

    assert content['form'] == data_str
    assert content['files'] == {'file': 'Something'}


def test_resp_headers(http_url, download_dir, d_config):

    query = {'resp_headers': f'resp_{d_config["backend"]}'}
    resptest = dm.DownloadManager(download_dir, **d_config)
    dltest = resptest._download(http_url, query=query)
    header_key = 'Content-Type'
    header_value = 'text/html; charset=utf-8'

    assert dltest[2] is not None

    header_dict = dltest[2].resp_headers

    assert len(header_dict) > 0
    assert header_key in header_dict
    assert header_value == header_dict[header_key]


def test_http_code(http_url, downloader):

    codes = [200, 301, 302, 400, 401, 404, 500]

    for code in codes:

        url = http_url + f'status/{code}' if code != 200 else http_url
        dl = downloader(dm.Descriptor(url))
        dl.setup()

        if code < 400 or downloader.__name__.startswith('Curl'):

            expected = 200 if code < 400 else code
            dl.download()

            assert dl.status_code == expected

            if code < 400:

                assert dl.success

            else:

                assert not dl.success
        else:

            with pytest.raises(requests.exceptions.HTTPError):

                dl.download()



def test_property_to_buffer(http_url, downloader):

    dl = downloader(dm.Descriptor(http_url), destination = False)
    dl.setup()
    dl.download()

    assert dl.to_buffer
    assert not dl.path_exists


def test_property_path_exists(http_url, download_dir, downloader):

    path = os.path.join(download_dir, "tempfile.txt")

    dl = downloader(dm.Descriptor(http_url), destination = path)
    dl.setup()
    dl.download()

    assert not dl.to_buffer
    assert dl.path_exists
    assert dl.ok


def test_filename(http_url, d_config):

    url = f'{http_url}/robots.txt'
    man = dm.DownloadManager(**d_config)
    d = man._download(url, dest = False)

    assert d[2].filename == 'robots.txt'


def test_filename_contdispos_buffer(http_url, d_config):

    url = (
        f'{http_url}/response-headers?'
        'Content-Type=text/plain;%20charset=UTF-8&'
        'Content-Disposition=attachment;%20filename%3d%22test.json%22'
    )

    man = dm.DownloadManager(**d_config)
    d = man._download(url, dest = False)

    assert d[2].filename == 'test.json'
    assert isinstance(d[3], io.BytesIO)


def test_filename_contdispos_disk(http_url, download_dir, d_config):

    url = (
        f'{http_url}/response-headers?'
        'Content-Type=text/plain;%20charset=UTF-8&'
        'Content-Disposition=attachment;%20filename%3d%22test.json%22'
    )

    # Creating a separate subdir for the different downloaders
    newpath = os.path.join(download_dir, d_config['backend'])

    man = dm.DownloadManager(path = newpath, **d_config)
    d = man._download(url)

    assert d[2].filename == 'test.json' # FIXME: Fails in requests :(
    assert os.path.exists(d[3])
    #assert d[3].endswith('.json') # TODO: should update filename in cache after download


def test_size(http_url, d_config):

    url = f'{http_url}/robots.txt?foobar=hello'
    man = dm.DownloadManager(**d_config)
    d = man._download(url, dest = False)

    assert d[2].size > 0
