import logging
from pathlib import Path
from datetime import datetime
from ionbeam.core.config_parser import parse_config
from ionbeam.core.bases import *
from ionbeam.metadata.db import init_db
from ionbeam.sources.meteotracker import MeteoTracker_API
from ionbeam.sources.meteotracker.metadata import add_meteotracker_track_to_metadata_store

logging.basicConfig(level = logging.INFO)

config_file = Path("~/git/IonBeam-Deployment/config/ionbeam").expanduser()
config, actions = parse_config(config_file,
                    config_path = "./",
                    data_path = "../data/",
                    offline = True,
                    environment  = "local",
                    sources = ["meteotracker"]
                    )

mt_source = actions[0]
init_db(config.globals)

api = MeteoTracker_API(mt_source.credentials, mt_source.api_headers)

timespan = (datetime(2021, 1, 1), datetime(2021, 1, 30))
sessions = api.query_sessions(timespan, items=10)
session = sessions[0]
data = api.get_session_data(session)

properties = {}
print(session, data.columns)
add_meteotracker_track_to_metadata_store(mt_source, session, data)
