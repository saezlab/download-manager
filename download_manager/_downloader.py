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
import logging
from ._misc import file_digest

try:
    import pycurl
except ImportError:
    pycurl = None

import requests

from cache_manager import _open
from cache_manager import utils as cmutils

from . import _data
from . import _curlopt
from . import _descriptor
from . import _log
from . import _misc

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

PARAMS = [
    'ssl_verifypeer',
    'ssl_verifyhost',
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
        logger.debug(
            'Initializing %s destination=%r',
            self.__class__.__name__,
            destination,
        )
        self.desc = desc
        self._downloaded = 0
        self._expected_size = 0
        self.http_code = 0
        self.set_destination(destination)


    def __del__(self):

        logger.debug('Finalizing %s', self.__class__.__name__)
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

        logger.debug('Resolved filename=%r', fname)
        return fname


    @property
    def ext(self) -> str | None:
        # TODO: Handle case when downloader gets file from cache

        ext = os.path.splitext(self.filename)[1]
        logger.debug('Resolved extension=%r', ext)
        return ext


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
            logger.debug('Computing %s checksum', digest)

            if self.path and os.path.exists(self.path):

                with open(self.path, 'rb') as f:

                    h = file_digest(f, digest)

            else:

                h = hashlib.new(digest)
                h.update(self._destination.getvalue())

            return h.hexdigest()
        logger.warning(
            'Skipping checksum because download is not successful (http_code=%s)',
            self.http_code,
        )


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

            _log('Closing destination.')
            logger.debug('Closing destination object type=%s', type(self._destination).__name__)
            self._destination.close()


    @property
    def ok(self) -> bool:
        """
        Checks whether the download was successful.

        Returns:
            `True`/`False` depending on the success of the download.
        """

        # TODO: Do we set up the `success` attribute somewhere?
        ok = self.success and (self.path_exists or self.to_buffer)
        logger.debug(
            'Evaluated ok=%s success=%s path_exists=%s to_buffer=%s',
            ok,
            self.success,
            self.path_exists,
            self.to_buffer,
        )
        return ok


    @property
    def success(self) -> bool:

        success = self.http_code == 200
        if not success:
            logger.debug('HTTP status is not success: %s', self.http_code)
        return success


    @property
    def path_exists(self) -> bool:

        exists = self.path and os.path.exists(self.path)
        logger.debug('Path exists check path=%r exists=%s', self.path, bool(exists))
        return exists


    @property
    def to_buffer(self) -> bool:

        destination = getattr(self, '_destination', None)
        to_buffer = isinstance(destination, io.BytesIO)
        logger.debug('Destination is buffer=%s', to_buffer)
        return to_buffer


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

                _log('Returning buffer')

                return self._destination

            else:

                _log(
                    f'Opening path {self.path} with '
                    f'{cmutils.serialize(kwargs)}'
                )
                self.opener = _open.Opener(self.path, **kwargs)

                return self.opener.result
        logger.warning('open called but downloader is not in ok state')


    def open_dest(self):
        """
        Sets up the destination for the download if available, otherwise
        defaults to buffer in memory.
        """

        if dest := self.destination:

            _log(f'Opening destination for writing {dest}')

            self._destination = open(dest, 'wb')
            logger.info('Opened file destination: %s', dest)

        else:

            _log(f'Creating buffer as download target')

            self._destination = io.BytesIO()
            logger.info('Created in-memory buffer destination')


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
        logger.debug('Set destination to %r', self.destination)


    def setup(self):
        """
        Sets up the downloader by calling all the set-up methods like
        initializing the download handler, configuration options, headers, etc.
        """

        _log('Setting up downloader')
        logger.info('Setting up downloader: %s', self.__class__.__name__)
        self.init_handler()
        self.set_options()
        self.open_dest()
        self.set_req_headers()
        self.set_resp_headers()
        self.set_progress()
        _log('Finished setting up the downloader')
        logger.debug('Downloader setup complete')


    @abc.abstractmethod
    def download(self) -> None:

        raise NotImplementedError()


    @abc.abstractmethod
    def init_handler(self) -> None:

        raise NotImplementedError()


    @abc.abstractmethod
    def set_options(self) -> None:

        raise NotImplementedError()


    def set_req_headers(self) -> None:

        _log(f'Setting request headers: {",".join(self.desc["headers"])}')
        logger.debug('Request header count=%d', len(self.desc['headers']))


    def set_progress(self) -> None:

        self._downloaded = 0
        self._expected_size = 0
        logger.debug('Progress counters reset')


    @abc.abstractmethod
    def set_resp_headers(self) -> None:

        self.resp_headers = []
        logger.debug('Response headers container initialized')


    def post_download(self) -> None:

        _log('Post-download workflow started')
        self.parse_resp_headers()
        self.get_http_code()
        _log(f'HTTP status code {self.http_code}')
        if self.http_code >= 400:
            logger.error('HTTP request failed with status=%s', self.http_code)
        elif self.http_code >= 300:
            logger.warning('HTTP request finished with redirect status=%s', self.http_code)
        else:
            logger.info('HTTP request finished successfully with status=%s', self.http_code)
        _log('Finished post-download workflow')


    def parse_resp_headers(self) -> None:

        self.resp_headers.update({
            key: self.parse_subheader(self.resp_headers.get(key, ''))
            for key in ['Content-Disposition', 'Content-Type']
        })
        _log(f'Parsing response headers {cmutils.serialize(self.resp_headers)}')
        logger.debug('Parsed response headers keys=%s', sorted(self.resp_headers.keys()))


    @staticmethod
    def parse_subheader(header: str) -> dict:

        logger.debug('Parsing response subheader: %r', header)
        return (
            header
                if isinstance(header, dict) else
            _misc.parse_header(header) or {}
        )


    @property
    def path(self):

        destination = getattr(self, '_destination', None)
        path = getattr(destination, 'name', None)
        logger.debug('Resolved destination path=%r', path)
        return path


    @property
    def size(self) -> int | None:

        if not self.ok and (epx := getattr(self, '_expected_size', 0)):

            logger.debug('Using expected size fallback=%s', epx)
            return epx

        if (path := self.path) and os.path.exists(path):

            size = os.path.getsize(path)
            logger.debug('Computed size from file path=%s size=%s', path, size)
            return size

        else:

            size = len(self._destination.getbuffer())
            logger.debug('Computed size from in-memory buffer size=%s', size)
            return size


    def _log_multipart(self) -> None:

        _log(f'Multipart form data {",".join(sorted(self.desc["multipart"].keys()))}')
        logger.info(
            'Multipart payload keys=%s',
            sorted(self.desc['multipart'].keys()),
        )


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
        logger.debug(
            'Curl progress downloaded=%s total=%s uploaded=%s upload_total=%s',
            downloaded,
            download_total,
            uploaded,
            upload_total,
        )


    def init_handler(self):
        """
        Initializes the `curl`-based donwload handler.
        """

        _log('Creating pycurl object')
        self.handler = pycurl.Curl()
        logger.info('Initialized pycurl handler')


    def download(self):
        """
        Performs the actual download and stores the result in the destination
        based on the information provided on the `Descriptor`.
        """

        self.setup()
        _log('Performing download')
        logger.info('Starting curl download to destination=%r', self.destination)
        try:
            self.handler.perform()
            self.post_download()
            logger.info('Curl download finished with http_code=%s', self.http_code)
        except Exception:
            logger.exception('Curl download failed')
            raise
        finally:
            self.handler.close()
            self._destination.seek(0)
            self.close_dest()
        _log('Download complete')

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
        logger.debug('Configured curl progress callbacks')


    def set_options(self):
        """
        Sets the options for the `curl`-based download handler inlcuding
        download methods (get/post) based on the provided `Descriptor` instance.
        """

        _log('Set parameters for Curl')
        logger.debug('Configuring curl options from PARAMS list')

        for param in PARAMS:

            if (value := self.desc[param]) is not None:
                _log(f'Curl parameter: {param} = {value}')

                self.handler.setopt(
                    getattr(self.handler, param.upper()),
                    _curlopt.process(param, value),
                )
                logger.debug('Applied curl option %s=%r', param, value)

        if self.desc['post']:

            _log('Setting HTTP POST')
            logger.info('Configuring curl POST request')

            if self.desc['multipart']:

                self._log_multipart()
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
                logger.debug('Configured curl multipart post fields')

            else:

                _log("JSON encoded post fields")

                data = (
                    json.dumps(self.desc['query'])
                    if self.desc['json']
                    else self.desc['qs']
                )

                self.handler.setopt(self.handler.POSTFIELDS, data)
                logger.debug('Configured curl POSTFIELDS payload type=%s', type(data).__name__)


    def set_req_headers(self):
        """
        Sets the request headers.
        """

        super().set_req_headers()

        self.handler.setopt(
            self.handler.HTTPHEADER,
            self.desc.headers_bytes,
        )
        logger.debug('Configured curl request headers')


    def set_resp_headers(self):
        """
        Sets the response headers.
        """

        super().set_resp_headers()
        self.handler.setopt(
            self.handler.HEADERFUNCTION,
            self.resp_headers.append,
        )
        logger.debug('Configured curl response header callback')


    def parse_resp_headers(self) -> None:

        if isinstance(self.resp_headers, list):
            logger.debug('Converting curl response headers list to dict')

            self.resp_headers = dict(
                (h.decode('utf-8').strip('\r\n').split(': ', 1) + [None])[:2]
                for h in self.resp_headers

            )

        super().parse_resp_headers()


    def get_http_code(self) -> None:

        self.http_code = self.handler.getinfo(self.handler.HTTP_CODE)
        logger.debug('curl HTTP code=%s', self.http_code)


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

        _log('Performing download')
        logger.info('Starting requests download to destination=%r', self.destination)
        req = self.request.prepare()

        try:
            with self.session.send(req, **self.send_args) as resp:

                self.response = resp
                self._expected_size = int(resp.headers.get('Content-Length', 0))
                logger.debug('Expected size from header=%s', self._expected_size)

                for chunk in resp.iter_content(1024):

                    self._destination.write(chunk)
                    self._downloaded =+ len(chunk)
        except Exception:
            logger.exception('Requests download failed')
            raise

        _log('Finished retrieving data')
        self._destination.seek(0)
        self.close_dest()
        self.post_download()
        logger.info('Requests download finished with http_code=%s', self.http_code)
        _log('Download complete')


    def init_handler(self):
        """
        Initializes the `requests`-based donwload handler and session.
        """

        _log('Creating Requests Session and Request')
        self.session = requests.Session()
        self.request = requests.Request()
        self.send_args = {}
        logger.info('Initialized requests Session and Request objects')


    def set_options(self):
        """
        Sets the options for the `requests`-based download handler inlcuding
        download methods (get/post) based on the provided `Descriptor` instance.
        """

        _log('Setting parameters for Requests')
        logger.debug('Configuring requests options')
        _log(f'Setting URL: `{self.desc["url"]}`')
        self.request.url = self.desc['url']
        self.send_args['allow_redirects'] = self.desc['followlocation']
        self.send_args['timeout'] = (
            self.desc['connecttimeout'],
            self.desc['timeout'],
        )
        logger.debug('Requests timeout config=%r', self.send_args['timeout'])

        if self.desc['post']:

            _log('Setting HTTP POST')
            logger.info('Configuring requests POST request')
            self.request.method = 'POST'

            if self.desc['multipart']:

                self._log_multipart()
                data = self.desc['multipart']['data']
                self.request.files = {
                    k: (v, open(v, 'rb'), mimetypes.guess_type(v)[0])
                    for k, v in self.desc['multipart']['files'].items()
                }
                logger.debug('Configured requests multipart files=%s', sorted(self.request.files.keys()))

            else:

                _log('JSON encoded POST fields')
                data = (
                    json.dumps(self.desc['query'])
                    if self.desc['json']
                    else self.desc['query']
                )

            self.request.data = data
            logger.debug('Configured requests POST data type=%s', type(data).__name__)

        else:

            self.request.method = 'GET'
            logger.debug('Configured requests GET request')

        _log(f'send_args: [{cmutils.serialize(self.send_args)}]')
        logger.debug('Requests send args=%r', self.send_args)

        # TODO: Figure out how to add these options in `requests` (if possible)
        #self.session.verify = self.desc['ssl_verifypeer']
        #if self.desc['ssl_verifypeer'] and self.desc['cainfo_override']:
        #    self.session.verify = self.desc['cainfo_override']


    def set_req_headers(self):
        """
        Sets the request headers.
        """

        super().set_req_headers()

        self.request.headers.update(self.desc.headers_dict)
        logger.debug('Configured requests headers count=%d', len(self.request.headers))


    def set_resp_headers(self) -> None:

        super().set_resp_headers()
        logger.debug('Requests response headers placeholder initialized')


    def parse_resp_headers(self) -> None:

        self.resp_headers = dict(self.response.headers)
        logger.debug('Captured requests response headers count=%d', len(self.resp_headers))
        super().parse_resp_headers()


    def get_http_code(self) -> None:

        self.http_code = self.response.status_code
        logger.debug('requests HTTP code=%s', self.http_code)
