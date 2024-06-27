from typing import Iterable

import dataclasses

from ..core.bases import (
    Parser,
    TabularMessage,
    FinishMessage,
)

from sqlalchemy import create_engine, URL
from sqlalchemy.orm import Session
from ..metadata import db
from ..metadata import meteotracker as mt

import logging
logger = logging.getLogger(__name__)

@dataclasses.dataclass
class GenerateMetaData(Parser):
    def __str__(self):
        return f"{self.__class__.__name__}({self.match})"

    def init(self, globals):
        super().init(globals)
        self.sql_engine = create_engine(URL.create(
            "postgresql+psycopg2",
            **self.globals.secrets["postgres_database"],
            host = self.globals.postgres_database["host"],
            port = self.globals.postgres_database["port"],
        ), echo = False)


    def process(self, msg : TabularMessage) -> Iterable[TabularMessage]:
            if isinstance(msg, FinishMessage):
                return
            
            with Session(self.sql_engine) as db_session:
                already_there, track = mt.add_meteotracker_track_to_metadata_store(db_session, msg)


            output_msg = TabularMessage(
                metadata=self.generate_metadata(
                     message=msg,
                     state = "metadatad",
                ),
                data=msg.data,
            )
            logger.debug(output_msg.metadata.state)

            output_message = self.tag_message(output_msg, msg)
            yield output_msg
