import pyfdb
from pathlib import Path
from ionbeam.core.config_parser import parse_globals
from pydantic.dataclasses import dataclass
from sqlalchemy.orm import Session
from ionbeam.metadata import db, id_hash


ionbeam = parse_globals(config_dir = Path(__file__).parents[1] / "config", environment = "ewc")

with Session(ionbeam.globals.sql_engine) as session:
    stations = session.query(db.Station).filter_by(
        platform = "Smart Citizen Kit"
    ).all()
    for station in stations:
        print(station)
        # session.delete(station)

    session.commit()


