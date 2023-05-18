import os

from ..core.plugins import find_plugin


class RawData:
    def __init__(self, location, id):
        self.location = location
        self.id = id

    def __str__(self):
        return f"RawData({self.id}: {self.location})"


class Source:
    """
    A source to feed data to other components.

    Implementors should override either __next__ or (more straightforwardly) the generate() generator function

    Should yield RawData objects
    """

    _generator = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._generator is None:
            self._generator = self.generate()
        return next(self._generator)

    def generate(self):
        raise NotImplementedError


def load_source(name: str, **kwargs) -> Source:
    klass = find_plugin(os.path.dirname(__file__), __name__, "source", name)
    return klass(**kwargs)
