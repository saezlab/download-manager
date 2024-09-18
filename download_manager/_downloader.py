from __future__ import annotations

__all__ = [
    'AbstractDownloader',
    'CurlDownloader',
    'PARAMS',
    'RequestsDownloader',
]

from typing import Any
import io
import os
import abc
import urllib
import json
import mimetypes

import pycurl
import requests

from . import _data
from . import _curlopt
from . import _descriptor

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
        self.setup()


    def __del__(self):

        self.close_dest()


    @property
    def url(self) -> str:

        return self.desc.url + ('' if self.post else f'?{self.qs}')


    def close_dest(self):

        if (
            hasattr(self, '_destination')
            and hasattr(self._destination, 'close')
            and not isinstance(self._destination, io.BytesIO)
        ):

            self._destination.close()


    def ok(self) -> bool:

        return getattr(self, 'success', False)


    def open_dest(self):

        if dest := self.destination:

            self._destination = open(dest, 'wb')

        else:

            self._destination = io.BytesIO()


    def param(self, key: str) -> Any:

        return self.desc[key]


    def set_destination(self, destination: str | None):

        self.destination = destination or self.param('destination')


    def setup(self):

        self.init_handler()
        self.set_options()
        self.open_dest()
        self.set_req_headers()
        self.set_resp_headers()


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


    def init_handler(self):

        self.handler = pycurl.Curl()


    def download(self):

        self.handler.perform()
        self.handler.close()
        self._destination.seek(0)
        self.close_dest()


    def open_dest(self):

        super().open_dest()

        self.handler.setopt(pycurl.WRITEFUNCTION, self._destination.write)


    def set_options(self):

        for param in PARAMS:

            if (value := self.desc[param]) is not None:

                self.handler.setopt(
                    getattr(self.handler, param.upper()),
                    _curlopt.process(param, value),
                )

        if self.desc['post']:

            if self.desc['multipart']:

                self.desc['headers'].append(
                    b'Content-Type: multipart/form-data'
                )

                self.handler.setopt(
                    self.handler.HTTPPOST,
                    [
                        (
                            name,
                            value
                            if typ == 'data'
                            else (pycurl.FORM_FILE, value)
                        )
                        for typ, params in self.desc['multipart'].items()
                        for name, value in params.items()
                    ]
                )

            else:

                data = (
                    json.dumps(self.desc['query'])
                    if self.desc['json']
                    else self.desc['qs']
                )

                self.handler.setopt(self.handler.POSTFIELDS, data)


    def set_req_headers(self):

        self.handler.setopt(
            self.handler.HTTPHEADER,
            self.desc['headers'],
        )


    def set_resp_headers(self):

        self.resp_headers = []
        self.handler.setopt(
            self.handler.HEADERFUNCTION,
            self.resp_headers.append,
        )


class RequestsDownloader(AbstractDownloader):
    """
    Requests download
    """

    def __init__(
        self,
        desc: _descriptor.Descriptor,
        destination: str | None = None,
    ):

        super().__init__(desc, destination)


    def download(self):

        req = self.request.prepare()

        with self.session.send(req, **self.send_args) as resp:

            self.response = resp
            resp.raise_for_status()

            for chunk in resp.iter_content(1024):

                self._destination.write(chunk)

        self._destination.seek(0)
        self.close_dest()


    def init_handler(self):
        """
        Initializes the `requests`-based donwload handler and session.
        """

        self.session = requests.Session()
        self.request = requests.Request()
        self.send_args = {}


    def set_options(self):
        """
        Sets the options for the `requests`-based download handler inlcuding
        download methods (get/post) based on the provided `Descriptor` instance.
        """

        self.request.url = self.desc['url']
        self.send_args['allow_redirects'] = self.desc['followlocation']
        self.send_args['timeout'] = (
            self.desc['connecttimeout'],
            self.desc['timeout'],
        )

        if self.desc['post']:

            self.request.method = 'POST'

            if self.desc['multipart']:

                data = self.desc['multipart']['data']
                self.request.files = {
                    k: (v, open(v, 'rb'), mimetypes.guess_type(v)[0])
                    for k, v in self.desc["multipart"]['files'].items()
                }

            else:

                data = (
                    json.dumps(self.desc['query'])
                    if self.desc['json']
                    else self.desc['query']
                )

            self.request.data = data

        else:

            self.request.method = 'GET'

        # TODO: Figure out how to add these options in `requests` (if possible)
        #self.session.verify = self.desc['ssl_verifypeer']
        #if self.desc['ssl_verifypeer'] and self.desc['cainfo_override']:
        #    self.session.verify = self.desc['cainfo_override']


    def set_req_headers(self):
        """
        Sets the request headers.
        """

        self.request.headers.update(self.desc.get_headers_dict())


    def set_resp_headers(self):
        """
        Sets the response headers. Not implemented - keeps defaults.
        """

        pass
        #self.resp_headers = self.response.headers
