from __future__ import annotations

import logging
import re
from typing import Any

import pycurl


# ---- Module level logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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

    logger.debug(f"Function `ensure_int` called with value: {value!r} and type {type(value)}")

    if isinstance(value, (int, bool)):
        logger.info(f"ensure_int: Value {value!r} is already int or bool, returning as int.")
        return int(value)

    for n in (value, f'CURL_{value}'):
        curl_int = getattr(pycurl, str(n).upper(), None)
        if curl_int is not None:
            logger.info(f"ensure_int: Resolved '{n}' to PyCurl int value {curl_int}.")
            return curl_int

    if isinstance(value, str):
        logger.warning(f"ensure_int: Could not resolve '{value!r}' to a PyCurl int, returning as bytes.")
        return value.encode('utf-8')

    logger.error(f"ensure_int: Unable to convert '{value!r}' to int or bytes. Returning None.")
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

    
    logger.debug(f"Function `http_version` called with ver: {ver!r}")
    ver = str(ver)

    if not re.match(r'^(?:curl_)?http_version', ver, re.IGNORECASE):
        logger.info(f"http_version: Formatting version string '{ver}' to PyCurl format.")
        ver = ver.replace('.', '_')
        ver = f'curl_http_version_{ver}'
    else:
        logger.info(f"http_version: Version string '{ver}' already in PyCurl format.")

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

    logger.debug(f"Function `process` called with key: {key!r}, value: {value!r}")

    if (proc := SYNONYMS.get(key, globals().get(key, None))):
        logger.info(f"process: Found processor for key '{key}'. Type: {type(proc)}")
        if isinstance(proc, dict):
            logger.debug(f"process: Using dict processor for key '{key}'.")
            value = proc.get(value, value)
        elif callable(proc):
            logger.debug(f"process: Using callable processor for key '{key}'.")
            value = proc(value)
    else:
        logger.warning(f"process: No processor found for key '{key}', using value as is.")

    result = ensure_int(value)
    if result is None:
        logger.error("process: Failed to process key '%s' with value %r", key, value)
    logger.debug(f"process: Result after ensure_int: {result!r}")
    return result


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)5s] [%(name)s:%(lineno)d]: %(message)s"
    )

    logger = logging.getLogger()

    logger.info(f"Experimenting with module {__name__}")

    from download_manager import _curlopt

    value = _curlopt.ensure_int(3.4)


    # Example 1: Standard HTTP version string
    ver1 = _curlopt.http_version("1.1")
    # Result: 'curl_http_version_1_1'

    # Example 2: Already formatted PyCurl option
    ver2 = _curlopt.http_version("curl_http_version_2")
    # Result: 'curl_http_version_2'

    # Example 3: With prefix and dot
    ver3 = _curlopt.http_version("http_version.2")
    # Result: 'curl_http_version_2'

    # Example 4: Lowercase input
    ver4 = _curlopt.http_version("http_version1.0")
    # Result: 'curl_http_version_1_0'

    # Example 5: Non-standard input
    ver5 = _curlopt.http_version("2.0")
    # Result: 'curl_http_version_2_0'

    # Example 6: Uppercase input
    ver6 = _curlopt.http_version("CURL_HTTP_VERSION_1_1")
    # Result: 'CURL_HTTP_VERSION_1_1'

    # Print results to verify
    print(ver1, ver2, ver3, ver4, ver5, ver6)

    result = _curlopt.process('http_version', '1.0')

    result = _curlopt.process('url', 'https://www.google.com')

    result = _curlopt.process('followlocation', True)
