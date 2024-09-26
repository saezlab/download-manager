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
import re
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
    Abstract class for individual download manager.

    Args:
        desc:
            Instance of `Descriptor` with the required information to perform a
            download.
        destination:
            Destination directory to download the file resulting from the
            download. Optional, defaults to `None`.

    Attrs:
        desc:
            The instance of the `Descriptor` associated to the download.
        destintation:
            The path or buffer of the destination of the download.
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
    def filename(self) -> str:

        if self.resp_headers:

            aux = self.resp_headers.get('Content-Disposition', '')
            re.match('filename="([^"]+)"', aux)

    @property
    def url(self) -> str:
        """
        Returns the full URL (i.e. including the query string in case of a GET
        request).

        Returns:
            The full URL as a string.
        """

        return self.desc.url + ('' if self.post else f'?{self.qs}')


    def close_dest(self):
        """
        Closes the destination writing function.
        """

        if (
            hasattr(self, '_destination')
            and hasattr(self._destination, 'close')
            and not isinstance(self._destination, io.BytesIO)
        ):

            self._destination.close()


    def ok(self) -> bool:
        """
        Checks whether the download was successful.

        Returns:
            `True`/`False` depending on the success of the download.
        """

        # TODO: Do we set up the `success` attribute somewhere?
        return getattr(self, 'success', False)


    def open_dest(self):
        """
        Sets up the destination for the download if available, otherwise
        defaults to buffer in memory.
        """

        if dest := self.destination:

            self._destination = open(dest, 'wb')

        else:

            self._destination = io.BytesIO()


    def param(self, key: str) -> Any:
        """
        Wrapper function that retrieves a requested parameter from the
        descriptor associated to the downloader instance.

        Args:
            key:
                Name of the parameter from the `Descriptor` to retrieve.

        Returns:
            The value of the requested parameter in the `Descriptor`.
        """

        return self.desc[key]


    def set_destination(self, destination: str | None):
        """
        Sets up the download destination property based on the given path,
        otherwise, takes it from the `Descriptor` instance.

        Args:
            destination:
                Path to the directory where the download should be performed.
        """

        self.destination = destination or self.param('destination')


    def setup(self):
        """
        Sets up the downloader by calling all the set-up methods like
        initializing the download handler, configuration options, headers, etc.
        """

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

        self.resp_headers = []


    def post_download(self) -> None:

        self.parse_resp_headers()


    @abc.abstractmethod
    def parse_resp_headers(self) -> None:

        raise NotImplementedError()


class CurlDownloader(AbstractDownloader):
    """
    Downloader based on the `pycurl` package.

    Args:
        desc:
            Instance of `Descriptor` with the required information to perform a
            download.
        destination:
            Destination directory to download the file resulting from the
            download. Optional, defaults to `None`.

    Attrs:
        handler:
            The instance of `pycurl.Curl` containing the configuration required
            for performing the download.
        resp_headers:
            The response headers after performing the download request.
        desc:
            Instance of `Descriptor` containing the relevant information to
            perform the download and configure the donwload handler.
    """

    def __init__(
            self,
            desc: _descriptor.Descriptor,
            destination: str | None = None,
    ):

        super().__init__(desc, destination)


    def init_handler(self):
        """
        Initializes the `curl`-based donwload handler.
        """

        self.handler = pycurl.Curl()


    def download(self):
        """
        Performs the actual download and stores the result in the destination
        based on the information provided on the `Descriptor`.

        """

        self.handler.perform()
        self.handler.close()
        self._destination.seek(0)
        self.close_dest()
        self.post_download()


    def open_dest(self):
        """
        Provides the `curl`-based handler with the destination for the download.
        """

        super().open_dest()

        self.handler.setopt(pycurl.WRITEFUNCTION, self._destination.write)


    def set_options(self):
        """
        Sets the options for the `curl`-based download handler inlcuding
        download methods (get/post) based on the provided `Descriptor` instance.
        """

        for param in PARAMS:

            if (value := self.desc[param]) is not None:

                self.handler.setopt(
                    getattr(self.handler, param.upper()),
                    _curlopt.process(param, value),
                )

        if self.desc['post']:

            if self.desc['multipart']:

                self.desc['headers'].append(
                    'Content-Type: multipart/form-data'
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
        """
        Sets the request headers.
        """

        self.handler.setopt(
            self.handler.HTTPHEADER,
            self.desc.headers_bytes,
        )


    def set_resp_headers(self):
        """
        Sets the response headers.
        """

        super().set_resp_headers()
        self.handler.setopt(
            self.handler.HEADERFUNCTION,
            self.resp_headers.append,
        )


    def parse_resp_headers(self) -> None:

        if isinstance(self.resp_headers, list):

            self.resp_headers = dict(
                (h.decode('utf-8').strip('\r\n').split(': ', 1) + [None])[:2]
                for h in self.resp_headers

            )


class RequestsDownloader(AbstractDownloader):
    """
    Downloader based on the `requests` package.

    Args:
        desc:
            Instance of `Descriptor` with the required information to perform a
            download.
        destination:
            Destination directory to download the file resulting from the
            download. Optional, defaults to `None`.

    Attrs:
        request:
            The instance of `requests.Request` containing the configuration
            specific to a request.
        session:
            An instance of `requests.Session` containing general configuration
            for the connection session.
        response:
            The response resulting from performing the request.
        send_args:
            Extra arguments sent alongside the request.
        desc:
            Instance of `Descriptor` containing the relevant information to
            perform the download and configure the donwload handler.
    """

    def __init__(
        self,
        desc: _descriptor.Descriptor,
        destination: str | None = None,
    ):

        super().__init__(desc, destination)


    def download(self):
        """
        Performs the actual download and stores the result in the destination
        based on the information provided on the `Descriptor`.
        """

        req = self.request.prepare()

        with self.session.send(req, **self.send_args) as resp:

            self.response = resp
            resp.raise_for_status()

            for chunk in resp.iter_content(1024):

                self._destination.write(chunk)

        self._destination.seek(0)
        self.close_dest()
        self.post_download()


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
                    for k, v in self.desc['multipart']['files'].items()
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

        self.request.headers.update(self.desc.headers_dict)


    def set_resp_headers(self) -> None:

        super().set_resp_headers()


    def parse_resp_headers(self) -> None:

        self.resp_headers = dict(self.response.headers)
