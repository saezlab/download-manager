import pycurl

import download_manager._curlopt as co

__all__ = [
    'test_process',
]

examples = [
    ('http_version', 'http_version_2', pycurl.CURL_HTTP_VERSION_2),
    ('http_version', '1.0', pycurl.CURL_HTTP_VERSION_1_0),
    ('http_version', None, pycurl.CURL_HTTP_VERSION_NONE),
    ('http_version', 'http_version_2TLS', pycurl.CURL_HTTP_VERSION_2TLS),
    ('http_version', 1.1, pycurl.CURL_HTTP_VERSION_1_1),
]


def test_process():
    for head, var, res in examples:
        assert co.process(head, var) == res
