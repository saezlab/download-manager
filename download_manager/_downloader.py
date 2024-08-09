from typing import Any
import os
import abc

import pycurl

from . import _data
from . import _descriptor

__all__ = [
    "CurlDownloader", "RequestsDownloader"
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


    def setup(self):

        self.handler = pycurl.Curl()
        self.set_url(url = url)
        self.handler.setopt(self.handler.SSL_VERIFYPEER, False)

        if DEBUG:

            self._log(
                'Following HTTP redirects: %s' % (
                    str(self.follow_http_redirect)
                )
            )

        self.handler.setopt(self.handler.FOLLOWLOCATION, self.follow_http_redirect)
        self.handler.setopt(self.handler.CONNECTTIMEOUT, self.connect_timeout)
        self.handler.setopt(self.handler.TIMEOUT, self.timeout)
        self.handler.setopt(self.handler.TCP_KEEPALIVE, 1)
        self.handler.setopt(self.handler.TCP_KEEPIDLE, 2)
        self.handler.setopt(self.handler.SSL_ENABLE_ALPN, self.alpn)

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
