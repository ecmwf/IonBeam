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
from typing import Iterable

# import codc as odc
from ..core.bases import BytesMessage, Encoder, TabularMessage

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class ParquetEncoder(Encoder):
    """
    """

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

        self.set_metadata = dataclasses.replace(self.set_metadata, state="encoded")
        self.fdb_schema = globals.fdb_schema
        

    def encode(self, msg: TabularMessage) -> Iterable[BytesMessage]:
        payload = msg.data.to_parquet(index=True)

        output_msg = BytesMessage(
            metadata=self.generate_metadata(
                msg,
                encoded_format="parquet",
            ),
            bytes=payload,
        )
        yield output_msg
