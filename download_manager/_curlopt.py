from __future__ import annotations

from typing import Any
import re

import pycurl

__all__ = [
    'SYNONYMS',
    'ensure_int',
    'http_version',
    'process',
]

SYNONYMS = {}


def ensure_int(value: Any) -> int | None:

    if isinstance(value, (int, bool)):

        return int(value)

    for n in (value, f'CURL_{value}'):

        if (curl_int := getattr(pycurl, n.upper(), None)) is not None:

            return curl_int

    if isinstance(value, str):

        return value.encode('utf-8')

    return None


def http_version(ver: str):

    ver = str(ver)

    if not re.match(r'^(?:curl_)?http_version', ver, re.IGNORECASE):

        ver = ver.replace('.', '_')
        ver = f'curl_http_version_{ver}'

    return ver


def process(key: str, value: Any) -> Any:

    if (proc := SYNONYMS.get(key, globals().get(key, None))):

        if isinstance(proc, dict):

            value = proc.get(value, value)

        elif callable(proc):

            value = proc(value)

    return ensure_int(value)
