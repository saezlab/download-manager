from typing import Any
import os
import abc

import pycurl

from . import _data, _descriptor, _curlopt

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
            'http_version',
            'ignore_content_length'
        ]

        for param in params:

            if (value := self.desc.param(param)) is not None:

                self.handler.setopt(
                    getattr(self.handler, param.upper()),
                    _curlopt.process(value),
                )

class RequestsDownloader(AbstractDownloader):
    """
    Requests download
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__(desc)
