from typing import Any
import os
import abc

from . import _data

__all__ = [
    ,
]

class AbstractDownloader(abc.ABC):
    """
    Single download manager
    """

    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def download(self) -> None:
        raise NotImplementedError()