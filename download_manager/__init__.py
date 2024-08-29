#!/usr/bin/env python

#
# This file is part of the `download_manager` Python module
#
# Copyright {% now 'utc', '%Y' %}
# Heidelberg University Hospital
#
# File author(s): OmniPath team (omnipathdb@gmail.com)
#
# Distributed under the GPLv3 license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

"""
Download manager for Python
"""

__all__ = [
    '__version__',
    '__author__',
]

from . import _data
from ._session import log, _log, session
from ._metadata import __author__, __version__
from ._descriptor import Descriptor
from ._downloader import *
from ._manager import *
from cache_manager import _log
