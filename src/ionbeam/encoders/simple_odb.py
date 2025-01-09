# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

import dataclasses
import logging
import os
import tempfile
from typing import Dict, Iterable

# import codc as odc
import pyodc as odc

from ..core.bases import BytesMessage, Encoder, TabularMessage

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SimpleODCEncoder(Encoder):
    """
    Encode data to ODB files.

    Args:
        output: A format string for the output path for each file.
        MARS_keys: Determines the output columns
    """
    output: str = "outputs/{source}/odb/{observation_variable}/{observation_variable}_{time_span.start}.odb"
    # copy_into_to_metadata: list[str] = dataclasses.field(default_factory=list)

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        self.metadata = dataclasses.replace(self.metadata, state="odc_encoded")
        self.fdb_schema = globals.fdb_schema

    def compute_file_metadata(self, message) -> Dict[str, str]:
        # additional_metadata = {
        #     "encoded_by": "IonBeam",  # TODO: add git commit hash or version here
        #     "IonBeam_git_hash": self.globals.code_source.git_hash,
        # }

        # additional_metadata["mars_request"] = json.dumps(mars_request.as_strings())
        # copy some data from the message metadata into the odb properties field
        # if msg.metadata:
        #     for key in ["source", "observation_variable"]:
        #         val = getattr(msg.metadata, key)
        #         if val is not None:
        #             additional_metadata[key] = val

        #     if msg.metadata.time_span is not None:
        #         additional_metadata["timeslice"] = str(msg.metadata.time_span.start.isoformat())

        # additional_metadata["columns"] = json.dumps(
        #     {
        #         key.name: {
        #             "description": key.column_description,
        #             # "unique_values": str(msg.data[key.name].unique()) if key.name in msg.data else None,
        #         }
        #         for key in self.MARS_keys
        #     }
        # )

        # # Copy additional data from the data itself into the properties
        # for key, search in self.copy_into_to_metadata:
        #     if search.startswith("data."):
        #         additional_metadata[key] = recursive_get(msg.data, search)
        #     elif search.startswith("metadata."):
        #         additional_metadata[key] = recursive_get(msg.metadata, search)
        #     else:
        #         raise ValueError(f"Unknown search prefix {search} should be data. or metadata.")

        return {}
        

    def encode(self, msg: TabularMessage) -> Iterable[BytesMessage]:

        
        odb_file_metadata = self.compute_file_metadata(msg)

        # Convert datetime columns to ISO 8601 string format
        df = msg.data.copy()
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col] \
                .dt.tz_convert('UTC') \
                .dt.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_filename = temp_file.name

        try:
            with open(temp_filename, 'wb') as write_end:
                odc.encode_odb(
                    df,
                    write_end,
                    properties=odb_file_metadata,
                )

            with open(temp_filename, 'rb') as read_end:
                payload = read_end.read()

            output_msg = BytesMessage(
                metadata=self.generate_metadata(
                    msg,
                    encoded_format="odb",
                ),
                bytes=payload,
            )
            yield self.tag_message(output_msg, msg)
        finally:
            try:
                os.remove(temp_filename)
            except OSError:
                pass
