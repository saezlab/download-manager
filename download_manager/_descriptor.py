from typing import Any
import os

from pypath_common import data

__all__ = [
    'Descriptor',
]


class Descriptor():
    """
    Describe the descriptor
    """

    def __init__(self, *args, **kwargs):
        self._param = dict()

        url_fname, *_ = list(args) + [None]
        self._param.update(kwargs)
        fname = url_fname or self.param('fname')

        if os.path.exists(fname):

            self.from_file(fname = fname)

        else:

            self._param['url'] = url_fname

        if not self.url:

            raise ValueError('Missing URL')


    def from_file(self, fname: str):
        self._param.update(data.load(fname))


    def param(self, key: str) -> Any:

        return self._param.get(key, None)


    @property
    def url(self) -> str:

        return self.param('url')
