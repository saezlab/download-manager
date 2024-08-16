from typing import Any
import io
import os
import abc
import urllib

import pycurl
import requests

from . import _data, _curlopt, _descriptor

__all__ = [
    'AbstractDownloader',
    'CurlDownloader',
    'RequestsDownloader',
]

PARAMS = [
    'ssl_verifypeer',
    'cainfo',
    'url',
    'followlocation',
    'connecttimeout',
    'timeout',
    'tcp_keepalive',
    'tcp_keepidle',
    'ssl_enable_alpn',
    'http_version',
    'ignore_content_length',
]


class AbstractDownloader(abc.ABC):
    """
    Single download manager
    """

    def __init__(
            self,
            desc: _descriptor.Descriptor,
            destination: str | None = None,
    ):
        super().__init__()
        self.desc = desc
        self.set_destination(destination)

    def setup(self):

        self.init_handler()
        self.set_options()
        self.open_dest()
        self.set_req_headers()
        self.set_resp_headers()


    def set_destination(self, destination: str | None):

        self.desc['destination'] = destination or self.param('destination')


    def open_dest(self):

        if dest := self.param('destination'):
            self.destination = open(dest, 'wb')

        else:
            self.destination = io.BytesIO()

    def param(self, key: str) -> Any:

        return self.desc[key]

    def close_dest(self):

        if (
            hasattr(self, 'destination')
            and hasattr(self.destination, 'close')
            and not isinstance(self.destination, io.BytesIO)
        ):
            self.destination.close()

    def __del__(self):

        self.close_dest()

    @property
    def url(self) -> str:

        return self.desc.url + ('' if self.post else f'?{self.qs}')

    @abc.abstractmethod
    def download(self) -> None:

        raise NotImplementedError()

    @abc.abstractmethod
    def init_handler(self) -> None:

        raise NotImplementedError()

    @abc.abstractmethod
    def set_options(self) -> None:

        raise NotImplementedError()

    @abc.abstractmethod
    def set_req_headers(self) -> None:

        raise NotImplementedError()

    @abc.abstractmethod
    def set_resp_headers(self) -> None:

        raise NotImplementedError()

class CurlDownloader(AbstractDownloader):
    """
    Curl download
    """

    def __init__(
            self,
            desc: _descriptor.Descriptor,
            destination: str | None = None,
        ):

        super().__init__(desc, destination)


    def download(self):

        self.handler.perform()
        self.handler.close()
        self.destination.seek(0)
        self.close_dest()


    def init_handler(self):

        self.handler = pycurl.Curl()


    def set_options(self):

        for param in PARAMS:

            if (value := self.desc[param]) is not None:

                self.handler.setopt(
                    getattr(self.handler, param.upper()),
                    _curlopt.process(param, value),
                )


    def open_dest(self):

        super().open_dest()

        self.handler.setopt(pycurl.WRITEFUNCTION, self.destination.write)


    def set_req_headers(self):

        self.handler.setopt(
            self.handler.HTTPHEADER,
            self.desc['headers'],
        )


    def set_resp_headers(self):

        self.resp_headers = []
        self.handler.setopt(
            self.handler.HEADERFUNCTION,
            self.resp_headers.append
        )

class RequestsDownloader(AbstractDownloader):
    """
    Requests download
    """

    def __init__(self, desc: _descriptor.Descriptor):

        super().__init__(desc)


    def init_handler(self):

        self.session = requests.Session()
        self.request = requests.Request()
        self.send_args = {}


    def set_options(self):

        self.request.url = self.desc['url']
        self.send_args['allow_redirects'] = self.desc['followlocation']
        self.send_args['timeout'] = (
            self.desc['connecttimeout'],
            self.desc['timeout']
        )

        self.session.verify = self.desc['ssl_verifypeer']

        if self.desc['ssl_verifypeer'] and self.desc['cainfo_override']:

            self.session.verify = self.desc['cainfo_override']


    def download(self):

        response = self.session.send(self.request, **self.send_args)


    def set_req_headers(self):

        self.request.headers.update(self.desc.get_headers_dict())


    def set_resp_headers(self):
        pass
        #self.resp_headers = self.handler.headers
