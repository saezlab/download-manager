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
    Dictionary-like class collecting all parameters that describe a download.
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

        self['baseurl'] = self['baseurl'] or self['url']
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


    def from_file(self, fname: str): # TODO: Specify format of the config file
        """
        Establishes all parameters of the descriptor from a given file.

        Args:
            fname:
                Path to the file with the parameters.
        """

        self._param.update(_data._module_data(fname))


    @property
    def headers_dict(self) -> dict:
        """
        Returns the request headers as a dictionary.

        Returns:
            A dictionary with the headers with key/value pairs as header
            name/value respectively.
        """

        return dict(elem.split(': ', maxsplit=1) for elem in self['headers'])


    def set_get_post(self):
        """
        Establishes the GET/POST parameters for the request as well as the URL
        accordingly.
        """

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
        Normalizes the format and sets the headers for the request.
        """

        hdr = self['headers']

        if isinstance(hdr, dict):

            hdr = [': '.join(h) for h in hdr.items()]

        hdr = misc.to_list(hdr)

        if self['json']:

            hdr.append('Content-Type: application/json')

        self['headers'] = hdr


    @property
    def headers_bytes(self) -> dict:
        """
        Returns the request headers as bytes.

        Returns:
            A list with the headers as byte-strings.
        """

        return [
            s.encode('ascii')
            if hasattr(s, 'encode') else s
            for s in self['headers'] or []
        ]


    def set_multipart(self):
        """
        Sets the `'multipart'` parameter for multiple-file downloads, which is a
        classifies the parts in a dictionary with keys `'data'` and `'files'`
        and values are dictionaries of key/value pairs for each of the
        corresponding items.
        """

        if self['multipart']:

            multipart = {'data': {}, 'files': {}}

            for k, v in self['multipart'].items():

                v = str(v)
                param_typ = 'files' if os.path.exists(v) else 'data'
                multipart[param_typ][k] = v

            self['multipart'] = multipart
