from ionbeam.metadata.db import Station, Property, Author
from ionbeam.sources.API_sources_base import DataChunk, DBDataChunk, DataStream
from ionbeam.core.config_parser import parse_single_action, parse_config
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import delete, text

config_file = Path("./config").expanduser()

action_yaml = """
class: SmartCitizenKitSource
mappings: []
copy_metadata_to_columns:
  external_station_id: device.id
"""

config, source = parse_single_action(config_file, action_yaml, 
                        environment  = "local",
                        die_on_error = False,
                                    )

with Session(config.globals.sql_engine) as db_session:
    with db_session.begin():
        db_session.execute(text("TRUNCATE TABLE property_station_association_table CASCADE"))
        db_session.execute(text("TRUNCATE TABLE station CASCADE"))
        db_session.execute(text("TRUNCATE TABLE property CASCADE"))
        db_session.execute(text("TRUNCATE TABLE author CASCADE"))