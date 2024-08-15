from typing import Any
import os
from collections import abc
import urllib

from . import _data

__all__ = [
    'Descriptor',
]


class Descriptor(abc.Mapping):
    """
    Describe the descriptor
    """

    def __init__(self, *args, **kwargs):
        self._param = dict()

        url_fname, *_ = list(args) + [None]
        self._param.update(kwargs)
        fname = url_fname or self['fname']

        if fname and os.path.exists(fname):

            self.from_file(fname = fname)

        else:

            self._param['url'] = self['url'] or url_fname

        if not self['url']:

            raise ValueError('Missing URL')

    def __iter__(self):
        return iter(self._param)

    def __contains__(self, value):
        return value in self._param.keys()

    def __len__(self):
        return len(self._param)
    
    def __getitem__(self, key: Any):

        return self._param.get(key, None)

    def __setitem__(self, key: Any, value: Any):

        self._param[key] = value

    def from_file(self, fname: str):

        self._param.update(_data._module_data(fname))

    def set_get_post(self):

        if self['query']:
            self['qs'] = urllib.parse.urlencode(self['query'])
