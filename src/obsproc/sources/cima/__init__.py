from .source import CIMASource

# Expose the Source object at obsproc.sources.cima.source so that load_source can find it
source = CIMASource
