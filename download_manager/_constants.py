import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DL_PARAMS_KEY = 'dl_params'
DL_DESC_KEY = 'dl_descriptor'

logger.debug(
    'Initialized cache key constants: DL_PARAMS_KEY=%s, DL_DESC_KEY=%s',
    DL_PARAMS_KEY,
    DL_DESC_KEY,
)
