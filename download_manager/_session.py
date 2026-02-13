import functools as _ft
import logging

from pypath_common import log as _read_log
from pypath_common import session as _session

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_get_session = _ft.partial(_session, 'download_manager')
log = _ft.partial(_read_log, 'download_manager')

session = _get_session()
_log = session._logger.msg
logger.debug('download_manager session initialized')
