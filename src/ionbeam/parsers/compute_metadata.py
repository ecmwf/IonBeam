import dataclasses
from typing import Iterable

from ..core.bases import Parser, TabularMessage
from ..metadata import id_hash


def parse_key(message, key):
    if key.startswith("const."):
        return key[len("const."):]
    
    if key.startswith("metadata."):
        if len(key.split(".")) > 2:
            raise NotImplementedError("Nested metadata keys not supported yet.")
        return getattr(message.metadata, key[len("metadata."):])
    
    elif key.startswith("data."):
        col = message.data[key[len("data.")]]
        assert col.nunique() == 1 
        return col.iloc[0]
    
    else:
        raise ValueError(f"Unknown search prefix {key} should be one of data, metadata, or const.")

@dataclasses.dataclass
class ComputeDateTime(Parser):


    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:

        
        assert msg.metadata.time_span is not None

        metadata = self.generate_metadata(msg)
        metadata.date = msg.metadata.time_span.start.strftime("%Y%m%d")
        metadata.time = msg.metadata.time_span.start.strftime("%H%M")

        output_msg = TabularMessage(data=msg.data, metadata=metadata)
        yield self.tag_message(output_msg, msg)

@dataclasses.dataclass
class ComputeMARSIdentifier(Parser):
    """
    """
    lookup: dict[str, str] = dataclasses.field(default_factory=dict)


    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:

        
        assert msg.metadata.time_span is not None

        mars_id = {}
        for key, value in self.lookup.items():
            mars_id[key] = parse_key(msg, value)
            
        mars_request, schema_branch = self.globals.fdb_schema.parse(mars_id)

        metadata = self.generate_metadata(msg)
        metadata.mars_id = mars_request

        output_msg = TabularMessage(data=msg.data, metadata=metadata)
        yield self.tag_message(output_msg, msg)

@dataclasses.dataclass
class ComputeInternalID(Parser):
    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        metadata = self.generate_metadata(msg)
        metadata.internal_id = id_hash(metadata.external_id)
        output_msg = TabularMessage(data=msg.data, metadata=metadata)
        yield self.tag_message(output_msg, msg)