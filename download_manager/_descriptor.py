from typing import Any
from collections import abc
import os
import urllib

from pypath_common import _misc as misc
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

        self['baseurl'] = self['url']
        self['ssl_verifypeer'] = False

        self.set_get_post()
        self.set_headers()


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

        if q := self['query']:

            self['qs'] = urllib.parse.urlencode(q)

        self['url'] = (
            self['baseurl']
                if self['post'] or not self['qs'] else
            f'{self["baseurl"]}?{self["qs"]}'
        )

    def set_headers(self):
        """
        Normalizes the format of the headers
        """

        hdr = self['headers']

        if isinstance(hdr, dict):
            hdr = [':'.join(h) for h in hdr.items()]

        hdr = misc.to_list(hdr)

        self['headers'] = [
            s.encode('ascii')
            if hasattr(s, 'encode')
            else s
            for s in hdr
        ]
