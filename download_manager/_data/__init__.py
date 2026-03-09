import functools as ft
import logging
from pypath_common import data as _data

#--- Module logger 
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_module_data = ft.partial(_data.load, module = 'download_manager')
