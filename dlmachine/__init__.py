"""
Download manager for Python
"""

__all__ = [
    '__version__',
    '__author__',
]

import logging

from . import _data
from ._session import session
from ._metadata import __author__, __version__
from ._descriptor import Descriptor
from ._downloader import *
from ._manager import *

# ---- Module level logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logger.debug(
    'dlmachine imported version=%s author=%s',
    __version__,
    __author__,
)
