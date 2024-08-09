from typing import Any
import os
import abc

import pycurl

from . import _data, _descriptor

__all__ = [
    'AbstractDownloader',
    'CurlDownloader',
    'RequestsDownloader',
]

class AbstractDownloader(abc.ABC):
    """
    Single download manager
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__()
        self.desc = desc

    @abc.abstractmethod
    def download(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def setup(self) -> None:
        raise NotImplementedError()


class CurlDownloader(AbstractDownloader):
    """
    Curl download
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__(desc)

    def download(self):
        pass


    def setup(self):

        self.handler = pycurl.Curl()

        params = [
            'ssl_verifypeer',
            'url',
            'followlocation',
            'connecttimeout',
            'timeout',
            'tcp_keepalive',
            'tcp_keepidle',
            'ssl_enable_alpn',
        ]

        for param in params:
            self.handler.setopt(
                getattr(self.handler, param.upper()),
                getattr(self.desc, param),
            )


        if not self.http2:

            self.handler.setopt(
                self.handler.HTTP_VERSION,
                pycurl.CURL_HTTP_VERSION_1_1,
            )

        if self.ignore_content_length:

            self.handler.setopt(self.handler.IGNORE_CONTENT_LENGTH, 136)


class RequestsDownloader(AbstractDownloader):
    """
    Requests download
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__(desc)
