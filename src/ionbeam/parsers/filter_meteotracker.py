#
# (C) Copyright 2023 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

import dataclasses
import logging
from typing import Iterable

from sqlalchemy.orm import Session

from ..core.bases import (
    FileMessage,
    FinishMessage,
    Parser,
    TabularMessage,
)
from ..metadata import db

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class FilterMeteoTracker(Parser):
    def init(self, globals):
        super().init(globals)


    def process(self, input_message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(input_message, FinishMessage):
            return

        external_id = input_message.metadata.unstructured["session"]["id"]

        with Session(self.globals.sql_engine) as db_session:
            track = db_session.query(db.Station).where(db.Station.external_id == external_id).one_or_none()
            if track:
                db_session.refresh(track)
                print(track, track.ingested)
                if track.ingested:
                    logger.debug(f"{external_id} already ingested, skipping")
                    return

        output_msg = TabularMessage(
            metadata=self.generate_metadata(message=input_message),
            data=input_message.data,
        )

        yield self.tag_message(output_msg, input_message)

@dataclasses.dataclass
class MeteoTrackerMarkIngested(Parser):
    def init(self, globals):
        super().init(globals)


    def process(self, input_message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(input_message, FinishMessage):
            return

        external_id = input_message.metadata.mars_request["external_id"].value

        with Session(self.globals.sql_engine) as db_session:
            track = db_session.query(db.Station).where(db.Station.external_id == external_id).one_or_none()
            if not track:
                raise ValueError(f"{external_id} should be in the SQL db but isn't")
            
            track.ingested = True
            logger.debug(f"{external_id} marked as ingested")

            db_session.commit()
            db_session.refresh(track)
            print(f"{track.ingested = }")

        with Session(self.globals.sql_engine) as db_session:
            track = db_session.query(db.Station).where(db.Station.external_id == external_id).one_or_none()
            print(f"{track.ingested = }")

        output_msg = FileMessage(
            metadata=self.generate_metadata(message=input_message),
        )

        yield self.tag_message(output_msg, input_message)


