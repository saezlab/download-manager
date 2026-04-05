from __future__ import annotations

import hashlib
import logging

from pkg_infra.utils import _misc

__all__ = ['file_digest', 'parse_header']

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def file_digest(fileobj, digest, /, *, _bufsize=2**18):
    """
    Hash the contents of a file-like object. Returns a digest object.

    *fileobj* must be a file-like object opened for reading in binary mode.
    It accepts file objects from open(), io.BytesIO(), and SocketIO objects.
    The function may bypass Python's I/O and use the file descriptor *fileno*
    directly.

    *digest* must either be a hash algorithm name as a *str*, a hash
    constructor, or a callable that returns a hash object.

    From Python standard library 3.11, copied for compatibility with older
    versions.
    """

    # On Linux we could use AF_ALG sockets and sendfile() to archive zero-copy
    # hashing with hardware acceleration.

    logger.debug('Starting file_digest digest=%r bufsize=%d', digest, _bufsize)

    if isinstance(digest, str):

        digestobj = hashlib.new(digest)
        logger.debug('Created digest object from algorithm name: %s', digest)

    else:

        digestobj = digest()
        logger.debug('Created digest object from callable')

    if hasattr(fileobj, "getbuffer"):

        # io.BytesIO object, use zero-copy buffer
        digestobj.update(fileobj.getbuffer())
        logger.info('Computed digest from in-memory buffer object')

        return digestobj

    # Only binary files implement readinto().
    if not (
        hasattr(fileobj, "readinto")
        and hasattr(fileobj, "readable")
        and fileobj.readable()
    ):
        logger.error('file_digest received non-readable binary file object: %r', fileobj)

        raise ValueError(
            f"'{fileobj!r}' is not a file-like object in binary reading mode."
        )

    # binary file, socket.SocketIO object
    # Note: socket I/O uses different syscalls than file I/O.
    buf = bytearray(_bufsize)  # Reusable buffer to reduce allocations.
    view = memoryview(buf)

    while True:
        size = fileobj.readinto(buf)

        if size == 0:

            break  # EOF

        digestobj.update(view[:size])

    logger.info('Computed digest from stream/file object')
    return digestobj


def parse_header(header: str, keys: list[str] | None = None) -> dict:
    """
    Parses a single header from an HTTP response from a string into a dictionary

    Args:
        header:
            The response header as a string
        keys:
            List of keys for values in the header without a associated key

    Returns:
        The processed header as a dictionary of key-value pairs
    """

    keys = _misc.to_list(keys) or range(123)
    parsed = dict(
        ([str(key)] + [el.strip(" '\"") for el in elem.split('=')])[-2:]
        for elem, key in zip(header.split(';'), keys)
    )
    logger.debug(
        'Parsed header into %d elements from raw header=%r',
        len(parsed),
        header,
    )
    return parsed
