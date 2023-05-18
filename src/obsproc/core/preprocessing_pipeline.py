
import re
from typing import Dict, Generator, Iterable

from ..parsers import ParsedData, load_parser
from ..sources import RawData


class PreprocessingPipeline:
    """
    A pipeline for processing incoming data

    :param parser: A configuration dictionary describing the parser to construct
    :param name: (Optional) the name of the pipeline to construct
    :param metadata: Additional metadata to annotate all of the data for this dataset
    :param match: A regex to identify matching data on the supplied id. Named groups identify metadata
    """
    def __init__(self,
                 parser: Dict,
                 match: str,
                 name='<unnamed>',
                 metadata=None):

        self.match = re.compile(match)
        self.name = name
        self.parser = load_parser(**parser)
        self.metadata = metadata or dict()

    def __str__(self):
        return f"PreprocessingPipeline[name={self.name}, parser={self.parser}]"

    def matches(self, rawdata: RawData):
        return self.match.match(rawdata.id)

    def annotate(self, parsed_data, extra_metadata) -> Generator[ParsedData, None, None]:
        for d in parsed_data:
            d.metadata.update(self.metadata)
            d.metadata.update(extra_metadata)
            yield d

    def process(self, rawdata):

        m = self.match.match(rawdata.id)
        assert m
        id_metadata = m.groupdict()

        parsed_data = self.parser.parse(rawdata)

        annotated_data = self.annotate(parsed_data, id_metadata)

        return annotated_data


class PreprocessingPipelines:

    _generator = None

    def __init__(self, config: Iterable[Dict], source: Iterable[RawData]):
        print(config)
        self.pipelines = [
            PreprocessingPipeline(**pipeline_config) for pipeline_config in config
        ]
        self.source = source

    def __iter__(self):
        return self

    def __next__(self):
        if self._generator is None:
            self._generator = self._generate()
        return next(self._generator)

    def _generate(self):
        for raw in self.source:
            for p in self.pipelines:
                if p.matches(raw):
                    for processed in p.process(raw):
                        yield processed
