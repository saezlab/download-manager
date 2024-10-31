import hashlib

from pypath_common import _misc

__all__ = ['file_digest', 'parse_header']


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

    if isinstance(digest, str):

        digestobj = hashlib.new(digest)

    else:

        digestobj = digest()

    if hasattr(fileobj, "getbuffer"):

        # io.BytesIO object, use zero-copy buffer
        digestobj.update(fileobj.getbuffer())

        return digestobj

    # Only binary files implement readinto().
    if not (
        hasattr(fileobj, "readinto")
        and hasattr(fileobj, "readable")
        and fileobj.readable()
    ):

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

    return dict(
        ([str(key)] + [el.strip(" '\"") for el in elem.split('=')])[-2:]
        for elem, key in zip(header.split(';'), keys)
    )

