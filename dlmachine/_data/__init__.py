import functools as ft
import logging
from pkg_infra import data as _data

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_raw_module_data = ft.partial(_data.load, module = 'dlmachine')


def _module_data(*args, **kwargs):

    logger.debug(
        'Loading packaged data with args=%r kwargs_keys=%s',
        args,
        sorted(kwargs.keys()),
    )
    data = _raw_module_data(*args, **kwargs)
    logger.info('Loaded packaged data object type=%s', type(data).__name__)
    return data
