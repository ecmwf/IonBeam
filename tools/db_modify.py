from pathlib import Path

from sqlalchemy.orm import Session

from ionbeam.core.config_parser import parse_globals
from ionbeam.metadata import db

ionbeam = parse_globals(config_dir = Path(__file__).parents[1] / "config", environment = "ewc")

with Session(ionbeam.globals.sql_engine) as session:
    stations = session.query(db.Station).filter_by(
        platform = "Smart Citizen Kit"
    ).all()
    for station in stations:
        print(station)
        # session.delete(station)


