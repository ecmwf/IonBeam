import dataclasses
import logging
from copy import copy
from typing import Iterable

from ..core.bases import DataMessage, Parser, TabularMessage
from ..metadata import id_hash

logger = logging.getLogger(__name__)

def parse_key(message, key):
    if key.startswith("const."):
        return key[len("const."):]
    
    if key.startswith("metadata."):
        if len(key.split(".")) > 2:
            raise NotImplementedError("Nested metadata keys not supported yet.")
        return getattr(message.metadata, key[len("metadata."):])
    
    elif key.startswith("data."):
        k = key[len("data."):]
        assert k in message.data, f"Column {k} not found in the input message."
        col = message.data[k]
        assert col.nunique() == 1, f"Column {key} does not have a unique value."
        return col.iloc[0]
    
    else:
        raise ValueError(f"Unknown search prefix {key} should be one of data, metadata, or const.")

@dataclasses.dataclass
class IdentityAction(Parser):
    def process(self, msg: DataMessage) -> Iterable[DataMessage]:
        output_msg = copy(msg)
        output_msg.metadata = self.generate_metadata(msg)
        output_msg = output_msg
        yield output_msg

@dataclasses.dataclass
class ComputeStationNameFromId(Parser):
    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        assert "station_id" in msg.data
        assert "platform" in msg.data
        msg.data["station_name"] = msg.data.platform.astype(str) + ": " + msg.data.station_id.astype(str)
        msg.metadata.columns["station_name"] = self.globals.canonical_variables_by_name["station_name"]
        yield msg

@dataclasses.dataclass
class ComputeChunkDateTime(Parser):
    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        assert msg.metadata.time_span is not None
        msg.data["chunk_date"] = msg.metadata.time_span.start.strftime('%Y%m%d')
        msg.data["chunk_time"] = msg.metadata.time_span.start.strftime('%H%M')
        msg.metadata.columns["chunk_date"] = self.globals.canonical_variables_by_name["chunk_date"]
        msg.metadata.columns["chunk_time"] = self.globals.canonical_variables_by_name["chunk_time"]
        yield msg

@dataclasses.dataclass
class ComputeMARSIdentifier(Parser):
    """
    """
    lookup: dict[str, str] = dataclasses.field(default_factory=dict)

    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        mars_id = {}
        for key, value in self.lookup.items():
            mars_id[key] = parse_key(msg, value)
            
        mars_request, schema_branch = self.globals.fdb_schema.parse(mars_id)
        msg.metadata.mars_id = mars_request
        yield msg

@dataclasses.dataclass
class ComputeStationId(Parser):
    def process(self, msg: TabularMessage) -> Iterable[TabularMessage]:
        assert "external_station_id" in msg.data
        msg.data["station_id"] = msg.data.external_station_id.astype("string").apply(id_hash).astype("string")
        msg.metadata.columns["station_id"] = self.globals.canonical_variables_by_name["station_id"]
        yield msg