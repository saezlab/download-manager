import functools as _ft
import logging

from pkg_infra.session import get_session as _get_session

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

session = _get_session(workspace=".")
logger.debug('dlmachine session initialized')
