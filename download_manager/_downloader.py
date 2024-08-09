from typing import Any
import os
import abc

from . import _data
from . import _descriptor

__all__ = [
    "CurlDownloader", "RequestsDownloader"
]

class AbstractDownloader(abc.ABC):
    """
    Single download manager
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__()
        self.desc = desc

    @abc.abstractmethod
    def download(self) -> None:
        raise NotImplementedError()


class CurlDownloader():
    """
    Curl download
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__(desc)


class RequestsDownloader():
    """
    Request download
    """

    def __init__(self, desc: _descriptor.Descriptor):
        super().__init__(desc)
