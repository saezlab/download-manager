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


def ensure_int(value: Any) -> int | str | None:
    """
    Attempts getting the numerical (integer) value of a PyCurl option. If not
    returns the name of option as string.

    Args:
        value:
            Integer or PyCurl option name to be converted to a valid integer
            corresponding to that option.

    Returns:
        The integer value of the PyCurl option. If none was found, returns back
        the option name or `None` if the provided value is not int, bool or str.
    """

    if isinstance(value, (int, bool)):

        return int(value)

    for n in (value, f'CURL_{value}'):

        if (curl_int := getattr(pycurl, n.upper(), None)) is not None:

            return curl_int

    if isinstance(value, str):

        return value.encode('utf-8')

    return None


def http_version(ver: str): # XXX: not used?
    """
    Ensures http version is correctly formatted according to the pre-defined
    available options in PyCurl.

    Args:
        ver:
            The http version option name.

    Returns:
        The correctly formatted http version option for PyCurl as a string.
    """

    ver = str(ver)

    if not re.match(r'^(?:curl_)?http_version', ver, re.IGNORECASE):

        ver = ver.replace('.', '_')
        ver = f'curl_http_version_{ver}'

    return ver


def process(key: str, value: Any) -> Any:
    """
    Standardizes PyCurl parameters.

    Args:
        key:
            Parameter name/synonym to standardize.
        value:
            Value of the parameter to standardize.

    Returns:
        Integer value corresponding to the PyCurl option (if available) or the
        option name otherwise.
    """

    if (proc := SYNONYMS.get(key, globals().get(key, None))):

        if isinstance(proc, dict):

            value = proc.get(value, value)

        elif callable(proc):

            value = proc(value)

    return ensure_int(value)
