import os

class Descriptor():
    """
    Describe the descriptor
    """

    def __init__(self, *args, **kwargs):
        self._param = dict()

        url_fname, *_ = args + [None]

        if os.path.exist(url_fname):
            self.from_file(fname = url_fname)

        else:
            self._param["url"] = url_fname

    def from_file(self, fname: str):
        pass
