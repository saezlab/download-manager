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
import urllib.parse as urlparse
import json
import mimetypes
import hashlib
from ._misc import file_digest

import pycurl
import requests

from cache_manager import _open

from . import _data
from . import _curlopt
from . import _descriptor
from . import _misc

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
        self._downloaded = 0
        self._expected_size = 0
        self.http_code = 0
        self.set_destination(destination)


    def __del__(self):

        self.close_dest()

    @property
    def filename(self) -> str | None:

        fname = (
            os.path.basename(urlparse.urlparse(self.desc['url']).path)
            or None
        )

        if isinstance(self.resp_headers, dict):

            fname = (
                self.resp_headers.
                get('Content-Disposition', {}).
                get('filename', fname)
            )

        return fname


    @property
    def ext(self) -> str | None:
        # TODO: Handle case when downloader gets file from cache

        return os.path.splitext(self.filename)[1]


    @property
    def sha256(self) -> str | None:

        return self.checksum()


    def checksum(self, digest: str = 'sha256') -> str | None:
        """
        Computes the file checksum (once downloaded). Defaults to SHA256 but
        other algorithms can be used (e.g. MD5, etc.).

        Args:
            digest:
                The algorithm used to compute the checksum. Defaults to
                `'sha256'`, other options available as implemented in the
                Python standard library `hashlib`.

        Returns:
            The resulting file checksum as a string.
        """

        if self.ok:

            if self.path and os.path.exists(self.path):

                with open(self.path, 'rb') as f:

                    h = file_digest(f, digest)

            else:

                h = hashlib.new(digest)
                h.update(self._destination.getvalue())

            return h.hexdigest()


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


    @property
    def ok(self) -> bool:
        """
        Checks whether the download was successful.

        Returns:
            `True`/`False` depending on the success of the download.
        """

        # TODO: Do we set up the `success` attribute somewhere?
        return self.success and (self.path_exists or self.to_buffer)


    @property
    def success(self) -> bool:

        return self.http_code == 200


    @property
    def path_exists(self) -> bool:

        return self.path and os.path.exists(self.path)


    @property
    def to_buffer(self) -> bool:

        return isinstance(self._destination, io.BytesIO)


    def open(self, **kwargs) -> str | IO | dict[str, str | IO] | None:
        """
        Open the downloaded file.

        Args:
            **kwargs:
                Keyword arguments are passed directly to the `Opener` class.

        Returns:
            The resulting opened file content. The type of content will depend
            on the passed arguments. See the `Opener` documentation for more
            details.
        """

        if self.ok:

            if self.to_buffer:

                return self._destination

            else:

                self.opener = _open.Opener(self.path, **kwargs)

                return self.opener.result


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
        self.set_progress()


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


    def set_progress(self) -> None:

        self._downloaded = 0
        self._expected_size = 0


    @abc.abstractmethod
    def set_resp_headers(self) -> None:

        self.resp_headers = []


    def post_download(self) -> None:

        self.parse_resp_headers()
        self.get_http_code()


    def parse_resp_headers(self) -> None:

        self.resp_headers.update({
            key: self.parse_subheader(self.resp_headers.get(key, ''))
            for key in ['Content-Disposition', 'Content-Type']
        })


    @staticmethod
    def parse_subheader(header: str) -> dict:

        return (
            header
                if isinstance(header, dict) else
            _misc.parse_header(header) or {}
        )


    @property
    def path(self):

        return getattr(self._destination, 'name', None)


    @property
    def size(self) -> int | None:

        if not self.ok and (epx := getattr(self, '_expected_size', 0)):

            return epx

        if (path := self.path) and os.path.exists(path):

            return os.path.getsize(path)

        else:

            return len(self._destination.getbuffer())


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


    def _progress(
        self,
        download_total: int,
        downloaded: int,
        upload_total: int,
        uploaded: int,
    ) -> None:

        self._downloaded = downloaded
        self._expected_size = download_total


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

        self.setup()
        self.handler.perform()
        self.post_download()
        self.handler.close()
        self._destination.seek(0)
        self.close_dest()


    def open_dest(self):
        """
        Provides the `curl`-based handler with the destination for the download.
        """

        super().open_dest()

        self.handler.setopt(pycurl.WRITEFUNCTION, self._destination.write)


    def set_progress(self):

        super().set_progress()
        self.handler.setopt(pycurl.XFERINFOFUNCTION, self._progress)
        self.handler.setopt(pycurl.PROGRESSFUNCTION, self._progress)
        self.handler.setopt(pycurl.NOPROGRESS, 0)


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

        super().parse_resp_headers()


    def get_http_code(self) -> None:

        self.http_code = self.handler.getinfo(self.handler.HTTP_CODE)


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

        self.setup()

        req = self.request.prepare()

        with self.session.send(req, **self.send_args) as resp:

            self.response = resp
            self._expected_size = int(resp.headers.get('Content-Length', 0))

            for chunk in resp.iter_content(1024):

                self._destination.write(chunk)
                self._downloaded =+ len(chunk)

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
        super().parse_resp_headers()


    def get_http_code(self) -> None:

        self.http_code = self.response.status_code
