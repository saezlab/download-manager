class Descriptor():
    """
    Describe the descriptor
    """

    def __init__(self, *args, **kwargs):
        self._param = dict()

        if 'fname' in kwargs:
            self.from_file()

    def from_file(self, fname: str):
        pass