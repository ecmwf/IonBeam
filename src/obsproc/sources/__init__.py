from pathlib import Path

from ..core.bases import Source
from ..core.plugins import find_plugin

from .meteotracker.online_source import MeteoTrackerSource
from .meteotracker.offline_source import MeteoTrackerOfflineSource
from .cima.source import CIMASource
from .multi_file import MultiFileSource
from .watch_directory import WatchDirectorySource

__all__ = ["MeteoTrackerSource", "MeteoTrackerOfflineSource", "CIMASource", "MultiFileSource", "WatchDirectorySource"]


def load_source(name: str, **kwargs) -> Source:
    klass = find_plugin(Path(__file__).parent, __name__, "source", name)
    return klass(**kwargs)
