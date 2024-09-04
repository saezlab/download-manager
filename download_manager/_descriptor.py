from __future__ import annotations

__all__ = [
    'Descriptor',
]

from typing import Any
from collections import abc
import os
import urllib

import certifi
from pypath_common import _misc as misc

from . import _data


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

        if not self['cainfo']:

            self['cainfo'] =  certifi.where()

        else:

            self['cainfo_override'] = self['cainfo']

        self['baseurl'] = self['url']
        self['followlocation'] = True

        self.set_get_post()
        self.set_headers()
        self.set_multipart()


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


    def get_headers_dict(self):

        return dict(
            elem.decode().split(': ', maxsplit=1)
            for elem in self['headers']
        )


    def set_get_post(self):

        if q := self['query']:

            self['qs'] = urllib.parse.urlencode(q)

        if self['json'] or self['multipart']:

            self['post'] = True

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
            hdr = [': '.join(h) for h in hdr.items()]

        hdr = misc.to_list(hdr)

        self['headers'] = [
            s.encode('ascii')
            if hasattr(s, 'encode')
            else s
            for s in hdr
        ]

        if self['json']:

            self['headers'].append(b'Content-Type: application/json')


    def set_multipart(self):

        if self['multipart']:

            multipart = {'data': {}, 'files': {}}

            for k, v in self['multipart'].items():

                v = str(v)
                param_typ = 'files' if os.path.exists(v) else 'data'
                multipart[param_typ][k] = v

            self['multipart'] = multipart
