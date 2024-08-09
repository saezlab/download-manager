import functools as ft
from pypath_common import data as _data

_module_data = ft.partial(_data.load, module = 'download_manager')
