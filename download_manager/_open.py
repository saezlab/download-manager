from __future__ import annotations

__all__ = [
    'ARCHIVES',
    'COMPRESSED',
    'Opener',
]

import io
import os
import re
import gzip
import struct
import tarfile
import zipfile

from pypath_common import _misc as _common
from pypath_common import _constants as _const

from cache_manager._session import _log

COMPRESSED =  {'gz', 'xz', 'bz2'}
ARCHIVES = {'zip', 'tar.gz', 'tar.bz2', 'tar.xz'}


class Opener:
    """
    Opens a file.

    This class opens a file, extracts it in case it is a gzip, tar.gz, tar.bz2
    or zip archive, selects the requested files if you only need certain files
    from a multifile archive, reads the data from the file, or returns the file
    pointer, as you request. It examines the file type and size. All these tasks
    are performed automatically upon instantiation.

    Args:
        path:
            Path to the file.
        ext:
            Extension of the file, such as "zip", "tar.gz", etc. Optional,
            defaults to `None`.
        needed:
            A list of paths to be extracted within an archive. If not provided,
            all paths will be included. Optional, defaults to `None`.
        large:
            Stores the file pointers instead of the contents of the files
            themselves. Optional, defaults to `True`.
        default_mode:
            Reading mode for the file objects: "r" (normal) or "rb" (binary).
            Optional, defaults to `"r"` (normal mode).
        encoding:
            Encoding for the stored file objects. Optional, defaults to `None`.

    Attrs:
        path:
            Path to the file.
        ext:
            Extension of the file.
        needed:
            Paths to be extracted for compressed files.
        large:
            Whether to store the file pointers instead of actual file contents.
        default_mode:
            File reading mode, "r" (normal) or "rb" (binary).
        encoding:
            Encoding type for text (e.g. 'utf-8', 'ascii', etc.).
        fileobj:
            Stores the file object instance.
        result:
            Content(s) of the file(s). Resulting content will depend whether the
            `large` attribute (i.e. file-object pointer or actual content) and
            in the case of compressed folders, the different files are stored
            as a dictionary where keys are file names and values are either file
            pointers or contents.
        size:
            Size of the file. In case of `.gz` format, the size of the
            compressed folder.
        sizes: Size of the files when compressd format, stored as a dictionary
            where keys are the file names.
        tarfile:
            Pointer to the tar-compressed file object (only when file is in
            `.tar` format).
        gzfile:
            Pointer to the gz-compressed file object (only when file is in
            `.gz` format).
        zipfile:
            Pointer to the zip-compressed file object (only when file is in
            `.zip` format).
    """

    #FIXME: attribute not used?
    _FORBIDDEN_CHARS = re.compile(r'[/\\<>:"\?\*\|]')

    def __init__(
            self,
            path: str,
            ext: str | None = None,
            needed: list[str] | None = None,
            large: bool = True,
            default_mode: str = 'r',
            encoding: str | None = None,
    ):

        for k, v in locals().items():

            if k == 'self':

                continue

            setattr(self, k, v)

        self.result = None
        self.set_type()
        self.open()
        self.extract()


    def __del__(self):

        self.close()


    def __iter__(self):

        self.fileobj.seek(0)

        return self.fileobj.__iter__()


    def close(self):
        """
        Closes the file.
        """

        if hasattr(self, 'fileobj') and hasattr(self.fileobj, 'close'):

            self.fileobj.close()


    def extract(self):
        """
        Calls the right extracting method for a compressed file according to the
        format.
        """

        getattr(self, 'open_%s' % self.type)()


    def open(self):
        """
        Loads the file object if exists on the disk. Stores the pointer under
        `fileobj` attribute. To obtain the contents, `extract` must be called.
        """

        if not os.path.exists(self.path):

            msg = f'No such file: `{self.path}`.'
            _log(msg)

            raise FileNotFoundError(msg)

        mode, encoding = (
            (self.default_mode, self.encoding)
            if self.type == 'plain'
            else ('rb', None)
        )
        self.fileobj = open(self.path, mode=mode, encoding=encoding)


    def open_gz(self):
        """
        Extracts files from `.gz` file. Resulting files are stored under the
        attribute `result`.
        """

        _log(f'Opening gzip file: {self.path}')

        self.fileobj.seek(-4, 2)
        self.size = struct.unpack('I', self.fileobj.read(4))[0]
        self.fileobj.seek(0)
        self.gzfile = gzip.GzipFile(fileobj=self.fileobj)

        if self.large:

            io.DEFAULT_BUFFER_SIZE = 4096
            self._gzfile_mode_r = io.TextIOWrapper(
                self.gzfile,
                encoding=self.encoding,
            )
            self.result = self.iterfile(
                self.gzfile
                if self.default_mode == 'rb'
                else self._gzfile_mode_r,
            )
            _log(f'Result is an iterator over the lines of {self.path}')

        else:

            self.result = self.gzfile.read()
            self.gzfile.close()
            _log(
                f'Data has been read from gzip file {self.path}. The file has '
                'been closed.',
            )


    def open_plain(self):
        """
        Opens a plain text file. Resulting file is stored under the attribute
        `result`.
        """

        _log(f'Opening plain text file {self.path}')

        self.size = os.path.getsize(self.fileobj.name)

        if self.large:

            self.result = self.iterfile(self.fileobj)

        else:

            self.result = self.fileobj.read()
            self.fileobj.close()
            _log(
                f'Contents of {self.path} has been read and the file has been '
                'closed.',
            )


    def open_tar(self):
        """
        Extracts files from `.tar` file. Resulting files are stored under the
        attribute `result`.
        """

        _log(f'Opening tar file: {self.path}')

        self._files = {}
        self.sizes = {}
        compr = self.ext.split('.')[-1]
        self.tarfile = tarfile.open(fileobj=self.fileobj, mode=f'r:{compr}')
        self._members = self.tarfile.getmembers()

        for m in self._members:

            if (
                (
                    self.needed is None or
                    m.name in self.needed
                )
                # Case m.size is 0 for dierctories
                and m.size != 0
            ):

                this_file = self.tarfile.extractfile(m)
                self.sizes[m.name] = m.size

                if self.large:

                    self._files[m.name] = this_file

                else:

                    _log(f'Reading contents of file from archive: `{m.name}`.')
                    self._files[m.name] = this_file.read()
                    this_file.close()

        if not self.large:

            self.tarfile.close()
            _log(f'File closed: `{self.path}`.')

        self.result = self._files


    def open_zip(self):
        """
        Extracts files from `.zip` file. Resulting files are stored under the
        attribute `result`.
        """

        _log(f'Opening zip file {self.path}')

        self._files_multipart = {}
        self.sizes = {}
        self.fileobj.seek(0)
        self.zipfile = zipfile.ZipFile(self.fileobj, 'r')
        self._members = self.zipfile.namelist()

        for i, m in enumerate(self._members):

            self.sizes[m] = self.zipfile.filelist[i].file_size

            if self.needed is None or m in self.needed:

                this_file = self.zipfile.open(m)

                if self.large:

                    if self.default_mode == 'rb':

                        # Keeping it in binary mode
                        self._files_multipart[m] = this_file

                    else:

                        # Wrapping the file for decoding
                        self._files_multipart[m] = io.TextIOWrapper(
                            this_file,
                            encoding=self.encoding,
                        )
                else:

                    self._files_multipart[m] = this_file.read()
                    this_file.close()

        if not self.large:

            self.zipfile.close()
            _log(
                f'Data has been read from zip file {self.path}. File has been '
                'closed',
            )

        self.result = self._files_multipart


    def set_type(self):
        """
        Determines the file type based on the extension.
        """

        ext = self.ext or _common.ext(self.path)
        ext = ext.strip('.')
        self.ext = 'tar.gz' if ext == 'tgz' else ext

        self.type = ext if ext in COMPRESSED | ARCHIVES else 'plain'
        self.type = 'tar' if self.type.startswith('tar') else self.type


    @staticmethod
    def iterfile(fileobj):
        """
        Returns an iterator over the lines of a file.
        """

        yield from fileobj
