from __future__ import annotations

__all__ = [
    'AbstractDownloader',
    'CurlDownloader',
    'PARAMS',
    'RequestsDownloader',
]

from typing import Any, IO
import io
import os
import abc
import re
import urllib
import urllib.parse as urlparse
import json
import mimetypes
import hashlib
import time
from ._misc import file_digest

import pycurl
import requests
import tqdm

from . import _utils

from . import _data
from . import _curlopt
from . import _descriptor
from . import _log
from . import _misc

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
            progress: bool = True,
    ):
        super().__init__()
        self.desc = desc
        self._downloaded = 0
        self._expected_size = 0
        self.http_code = 0
        self._progress_bar = None
        self._show_progress = progress
        self._progress_pending = 0
        self._progress_min_chunk = 64 * 1024  # flush at least every 64KB
        self._progress_min_interval = 0.1  # or every 100ms
        self._last_progress_update = 0.0
        self.set_destination(destination)


    def __del__(self):

        self.close_progress()
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

            _log('Closing destination.')
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

                _log('Returning buffer')

                return self._destination

            else:

                _log(
                    f'Opening path {self.path} with '
                    f'{_utils.serialize(kwargs)}'
                )

                # Simple file opening - users can handle decompression themselves
                return open(self.path, 'rb')


    def open_dest(self):
        """
        Sets up the destination for the download if available, otherwise
        defaults to buffer in memory.
        """

        if dest := self.destination:

            _log(f'Opening destination for writing {dest}')

            self._destination = open(dest, 'wb')

        else:

            _log(f'Creating buffer as download target')

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

        _log('Setting up downloader')
        self.init_handler()
        self.set_options()
        self.open_dest()
        self.set_req_headers()
        self.set_resp_headers()
        self.set_progress()
        _log('Finished setting up the downloader')


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


    def set_progress(self) -> None:

        self._downloaded = 0
        self._expected_size = 0
        self._progress_pending = 0
        self._last_progress_update = time.monotonic()

        # Ensure any prior progress bar is cleaned up before starting anew
        if self._progress_bar is not None:
            self.close_progress()

    def init_progress(self, total: int | None = None, desc: str = 'Downloading') -> None:
        """
        Initialize the tqdm progress bar.

        Args:
            total: Total size of the download in bytes. If None, uses indeterminate progress.
            desc: Description to display for the progress bar.
        """

        if not self._show_progress:
            return

        if self._progress_bar is None:
            self._progress_bar = tqdm.tqdm(
                total=total,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=desc,
                leave=True,
                dynamic_ncols=True,
            )
            return

        # Update description if it changed
        if desc and self._progress_bar.desc != desc:
            self._progress_bar.set_description(desc, refresh=False)

        if total is not None:
            self._progress_bar.total = total

    def update_progress(self, n: int, force: bool = False) -> None:
        """
        Update the progress bar by n bytes.

        Args:
            n: Number of bytes to add to progress.
            force: Flush buffered progress immediately regardless of thresholds.
        """

        if not self._show_progress:
            return

        if n > 0:
            if self._progress_bar is None:
                # Total may still be unknown at this point
                total = self._expected_size or None
                self.init_progress(total=total)

            if self._progress_bar is None:
                return

            self._progress_pending += n
        elif self._progress_bar is None:
            # Nothing to flush and no bar to update
            return

        now = time.monotonic()
        should_flush = force or (
            self._progress_pending >= self._progress_min_chunk
            or (now - self._last_progress_update) >= self._progress_min_interval
        )

        if not should_flush:
            return

        if self._progress_pending > 0:
            self._progress_bar.update(self._progress_pending)
            self._progress_pending = 0

        if force:
            self._progress_bar.refresh()

        self._last_progress_update = now

    def set_progress_total(self, total: int) -> None:
        """
        Set or update the total size for the progress bar.

        Args:
            total: Total size in bytes.
        """

        if total <= 0 or not self._show_progress:
            return

        # Ensure progress bar exists and reflects the new total
        self.init_progress(total=total)

    def close_progress(self) -> None:
        """
        Close and cleanup the progress bar.
        """

        # Flush any buffered progress before closing
        self.update_progress(0, force=True)

        if self._progress_bar is not None:
            self._progress_bar.close()
            self._progress_bar = None


    @abc.abstractmethod
    def set_resp_headers(self) -> None:

        self.resp_headers = []


    def post_download(self) -> None:

        _log('Post-download workflow started')
        self.parse_resp_headers()
        self.get_http_code()
        _log(f'HTTP status code {self.http_code}')
        _log('Finished post-download workflow')


    def parse_resp_headers(self) -> None:

        self.resp_headers.update({
            key: self.parse_subheader(self.resp_headers.get(key, ''))
            for key in ['Content-Disposition', 'Content-Type']
        })
        _log(f'Parsing response headers {_utils.serialize(self.resp_headers)}')


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


    def _log_multipart(self) -> None:

        _log(f'Multipart form data {",".join(sorted(self.desc["multipart"].keys()))}')


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
        progress: bool = True,
    ):

        super().__init__(desc, destination, progress)


    def _progress(
        self,
        download_total: int,
        downloaded: int,
        upload_total: int,
        uploaded: int,
    ) -> None:

        # Ensure a progress bar exists before attempting to update it
        if self._show_progress and self._progress_bar is None:
            initial_total = download_total if download_total > 0 else None
            self.init_progress(total=initial_total)

        # Update total size when it becomes available
        if download_total > 0 and self._expected_size != download_total:
            self._expected_size = download_total
            self.set_progress_total(download_total)

        # Update progress
        if downloaded > self._downloaded:
            delta = downloaded - self._downloaded
            force_flush = download_total > 0 and downloaded >= download_total
            self.update_progress(delta, force=force_flush)
        elif download_total > 0 and downloaded >= download_total:
            # Ensure buffered progress is flushed when finished
            self.update_progress(0, force=True)

        self._downloaded = downloaded


    def init_handler(self):
        """
        Initializes the `curl`-based donwload handler.
        """

        _log('Creating pycurl object')
        self.handler = pycurl.Curl()


    def download(self):
        """
        Performs the actual download and stores the result in the destination
        based on the information provided on the `Descriptor`.
        """

        self.setup()
        _log('Performing download')
        self.handler.perform()
        self.post_download()
        self.handler.close()
        self.close_progress()
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


    def set_options(self):
        """
        Sets the options for the `curl`-based download handler inlcuding
        download methods (get/post) based on the provided `Descriptor` instance.
        """

        _log('Set parameters for Curl')

        for param in PARAMS:

            if (value := self.desc[param]) is not None:
                _log(f'Curl parameter: {param} = {value}')

                self.handler.setopt(
                    getattr(self.handler, param.upper()),
                    _curlopt.process(param, value),
                )

        if self.desc['post']:

            _log('Setting HTTP POST')

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

            else:

                _log("JSON encoded post fields")

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

        super().set_req_headers()

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
        progress: bool = True,
    ):

        super().__init__(desc, destination, progress)


    def download(self):
        """
        Performs the actual download and stores the result in the destination
        based on the information provided on the `Descriptor`.
        """

        self.setup()

        _log('Performing download')
        req = self.request.prepare()

        with self.session.send(req, **self.send_args) as resp:

            self.response = resp
            self._expected_size = int(resp.headers.get('Content-Length', 0))

            progress_desc = 'Downloading'
            if self._show_progress:
                url_path = urlparse.urlparse(self.desc['url']).path
                inferred_name = self.filename or os.path.basename(url_path)
                if inferred_name:
                    progress_desc = f'Downloading {inferred_name}'

            # Ensure progress bar exists even if total is unknown
            self.init_progress(
                total=self._expected_size or None,
                desc=progress_desc,
            )

            # Update progress bar total if known
            if self._expected_size > 0:
                self.set_progress_total(self._expected_size)

            for chunk in resp.iter_content(chunk_size=64 * 1024):

                if not chunk:
                    continue

                self._destination.write(chunk)
                chunk_size = len(chunk)
                self._downloaded += chunk_size

                # Update progress bar
                self.update_progress(chunk_size)

            # Flush any buffered progress before closing the response context
            self.update_progress(0, force=True)

        _log('Finished retrieving data')
        self.close_progress()
        self._destination.seek(0)
        self.close_dest()
        self.post_download()
        _log('Download complete')


    def init_handler(self):
        """
        Initializes the `requests`-based donwload handler and session.
        """

        _log('Creating Requests Session and Request')
        self.session = requests.Session()
        self.request = requests.Request()
        self.send_args = {}


    def set_options(self):
        """
        Sets the options for the `requests`-based download handler inlcuding
        download methods (get/post) based on the provided `Descriptor` instance.
        """

        _log('Setting parameters for Requests')
        _log(f'Setting URL: `{self.desc["url"]}`')
        self.request.url = self.desc['url']
        self.send_args['allow_redirects'] = self.desc['followlocation']
        self.send_args['timeout'] = (
            self.desc['connecttimeout'],
            self.desc['timeout'],
        )
        # Stream response so progress updates while downloading, not after
        self.send_args['stream'] = True

        if self.desc['post']:

            _log('Setting HTTP POST')
            self.request.method = 'POST'

            if self.desc['multipart']:

                self._log_multipart()
                data = self.desc['multipart']['data']
                self.request.files = {
                    k: (v, open(v, 'rb'), mimetypes.guess_type(v)[0])
                    for k, v in self.desc['multipart']['files'].items()
                }

            else:

                _log('JSON encoded POST fields')
                data = (
                    json.dumps(self.desc['query'])
                    if self.desc['json']
                    else self.desc['query']
                )

            self.request.data = data

        else:

            self.request.method = 'GET'

        _log(f'send_args: [{_utils.serialize(self.send_args)}]')

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


    def set_resp_headers(self) -> None:

        super().set_resp_headers()


    def parse_resp_headers(self) -> None:

        self.resp_headers = dict(self.response.headers)
        super().parse_resp_headers()


    def get_http_code(self) -> None:

        self.http_code = self.response.status_code
