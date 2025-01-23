import os
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Literal, Optional

import argparse
import duckdb
import pandas as pd
import pyfdb
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse
from frozendict import frozendict
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
import uvicorn

from ..core.config_parser import parse_globals
from ..core.mars_keys import FDBSchemaFile
from ..core.time import TimeSpan
from ..metadata.db import Station
from ..writers.fdb_writer import install_metkit_overlays

parser = argparse.ArgumentParser(
    prog="IonBeam REST API",
    description="Beam IoT data around",
    epilog="See https://github.com/ecmwf/ionbeam for more info.",
)
parser.add_argument(
    "config_folder",
    help="Path to the config folder.",
    type=Path,
)

parser.add_argument(
    "--port",
    type=int,
    default=8000,
    help="The port to serve requests on, defaults to 8000",
)

parser.add_argument(
    "--host",
    type=str,
    default="127.0.0.1",
    help="The host address to use, defaults to loopback.",
)

parser.add_argument(
    "--environment",
    "-e",
    help="Which environment to use, local, dev or test",
)

global fdb
fdb = None

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# The problem with this is that if you drop a key then it will ignore all keys deeper than that in the schema
default_mars_request = {
      "class" : "rd",
      "expver" :  "xxxx",
      "stream" : "lwda",
      "aggregation_type": "by_time",
}

def override_date_in_mars_request(start_time : datetime | None, end_time : datetime | None, mars_request : dict[str, str]):
    "Override the date in the mars request with the start and end time"
    
    if start_time is not None and end_time is not None:
        start_date = start_time.strftime("%Y%m%d")
        end_date = end_time.strftime("%Y%m%d")
        if start_date == end_date: date = start_date
        else: date = f"{start_date}/to/{end_date}/by/1"
        mars_request["date"] = date


def sort_and_filter_by_datetime_span(matches, start_time, end_time):
    "Filter the matches by the datetime span"
    datetime_filtered_matches = []
    for m in matches:
        date = m["keys"]["date"]
        time = m["keys"]["time"]

        # all internal dates are in UTC so we can safely do .replace(tzinfo=UTC) here
        dt = datetime.strptime(date + time, "%Y%m%d%H%M").replace(tzinfo=UTC)
        m["datetime"] = dt
        
        if start_time is not None and end_time is not None:
            if not (start_time <= dt <= end_time):
                continue
        
        datetime_filtered_matches.append(m)

    return sorted(datetime_filtered_matches, key = lambda x : x["datetime"])

def remove_duplicates(matches) -> list:
    "Remove duplicates from the matches, returns only the newest version of each file"
    known = dict()
    for m in matches:
        m["m_time"] = os.path.getmtime(m["path"])
        mars_id = frozendict(m["keys"])
        if mars_id in known:
            if known[mars_id]["m_time"] < m["m_time"]:
                known[mars_id] = m
        else:
            known[mars_id] = m
    return list(known.values())

# Reusable Query parameters
start_time_query = Query(
    None,
    description="The start datetime for the data retrieval in ISO 8601 format.",
    example="2024-01-01T00:00:00",
)

end_time_query = Query(
    None,
    description="The end datetime for the data retrieval in ISO 8601 format.",
    example="2024-12-31T23:59:59",
)

station_id_query = Query(
    None,
    description="The station id for the data retrieval.",
    example="1c001400c38e9a8b",
)

@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse("favicon.ico")


class ListResult(BaseModel):
    mars_request: dict[str, str] = Field(
        ..., description="Parameters used for the MARS request"
    )
    url: str = Field(..., description="The URL to the data file")
    datetime: datetime

    class Config:
        json_schema_extra = {
            "example":   {
                "mars_request": {
                "class": "rd",
                "expver": "xxxx",
                "stream": "lwda",
                "aggregation_type": "tracked",
                "platform": "meteotracker",
                "date": "20241212",
                "station_id": "1c001400c38e9a8b",
                "time": "0735"
                },
                "url": "/api/v1/retrieve?class=rd&expver=xxxx&stream=lwda&aggregation_type=tracked&platform=meteotracker&date=20241212&station_id=1c001400c38e9a8b&time=0735"
            }
        }

class FDBError(BaseModel):
    details: str
    exception: str | None = None

@app.get(
    "/api/v1/list",
    summary="List Data",
    description=(
        "This endpoint lists data based on the provided mars request and date range."
    ),
    response_description="A JSON formatted containing the data granules that match the query.",
    responses={
        200: {"model": ListResult},
        400: {"model": FDBError}
        }
)
async def fdb_list(
    request: Request,
    start_time: Optional[datetime] = start_time_query,
    end_time: Optional[datetime] = end_time_query,
    station_id: Optional[str] = station_id_query,
):
    args = {"start_time", "end_time", "station_id"}
    mars_request = default_mars_request | {k : v for k,v in request.query_params.items() if k not in args}
    override_date_in_mars_request(start_time, end_time, mars_request)

    try:
        response = list(fdb.list(mars_request, keys=True, duplicates = False))
        response = sort_and_filter_by_datetime_span(response, start_time, end_time)
        response = remove_duplicates(response)

        return [
            ListResult(
                datetime = r["datetime"],
                mars_request=r["keys"],
                url="/api/v1/retrieve?" + "&".join(f"{k}={v}" for k,v in r["keys"].items()),
            )
            for r in response
        ]
    except pyfdb.FDBException as e:
        raise HTTPException(status_code=500, detail=dict(
            message = "FDB list failed",
            error = str(e),
            ))


@app.get("/api/v1/stations")
async def get_stations(
    external_id : str | None = Query(
        None,
        description="The id of the station from whichever source it was ingested from",
        example="",
    ),
    station_id : str | None = Query(
        None,
        description="The id of the station within the IonBeam system",
        example="",
    ),
    start_time: Optional[datetime] = start_time_query,
    end_time: Optional[datetime] = end_time_query,
    platform: Optional[str] = Query(
        None,
        description="The source of the station date",
        example="meteotracker",
    )
):
    try:
        filters = []
        if external_id is not None:
            filters.append(Station.external_id == external_id)

        if station_id is not None:
            filters.append(Station.internal_id == station_id)

        if platform is not None:
            filters.append(Station.platform == platform)

        if start_time is not None or end_time is not None:
            if start_time is None:
                start_time = datetime.min.replace(tzinfo=UTC)  # Earliest possible date
            if end_time is None:
                end_time = datetime.max.replace(tzinfo=UTC)  # Latest possible date
            
            timespan = TimeSpan(start_time, end_time)
            filters.append(Station.timespan_overlaps(timespan))

        with Session(ionbeam.globals.sql_engine) as db_session:
            query = db_session.query(Station).filter(*filters)
            return [s.as_json() for s in query.all()
                    if s.time_span is not None]
    
    except pyfdb.FDBException as e:
        raise HTTPException(status_code=500, detail=dict(
            message = "FDB list failed",
            error = str(e),
            ))
    




@app.get(
    "/api/v1/retrieve",
    summary="Retrieve Data",
    description=(
        "This endpoint retrieves data based on the provided filters and date range. "
        "You can specify an optional SQL-like filter and choose the output format."
    ),
    response_description="A JSON or other formatted response containing the filtered data.",
)
async def fdb_retrieve(
    request: Request,
    format: Optional[Literal["json", "csv", "parquet"]] = Query(
        "json",
        description="Output format for the response. Default is JSON. Options are 'json', 'csv', 'odb'.",
        example="json"
    ),
    filter: str = Query(
        None,
        description="An SQL filter to apply to the retrieved data.",
        example="status = 'active' AND age > 30"
    ),
    start_time: Optional[datetime] = start_time_query,
    end_time: Optional[datetime] = end_time_query,
    station_id: Optional[str] = station_id_query,
):
    try:
        args = {"format", "filter", "start_time", "end_time", "station_id"}
        mars_request = default_mars_request | {k : v for k,v in request.query_params.items() if k not in args}
        
        if start_time is not None and start_time.tzinfo is None:
            raise HTTPException(status_code=400, detail="start_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'")
        if end_time is not None and end_time.tzinfo is None:
            raise HTTPException(status_code=400, detail="end_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'")
        
        override_date_in_mars_request(start_time, end_time, mars_request)

        matches = list(fdb.list(mars_request, keys=True))

        matches = sort_and_filter_by_datetime_span(matches, start_time, end_time)

        matches = remove_duplicates(matches)


        if len(matches) == 0:
             raise HTTPException(status_code=404, detail="No data found for the given query.")

        # Convert the buffer to a datafame so we can filter on it or convert to other formats
        # delete the buffers
        result = None
        try:
            if len(matches) == 1:
                path = matches[0]["path"]
                result = pd.read_parquet(path)
            if len(matches) > 200:
                raise HTTPException(status_code=403, 
                                    detail="This request would return more than 200 data granules, please request a smaller time span repeatedly.")
                
            else:
                dfs = [pd.read_parquet(m["path"]) for m in matches]
                
                if station_id:
                    dfs = [df[df["station_id"] == station_id]
                            for df in dfs]
                    
                result = pd.concat(dfs)
        except Exception as e:
            if app.debug:
                raise e
            return dict(error="Failed to read data", exception=str(e)), 500


        if filter is not None:
            with duckdb.connect() as con:
                result = con.query(filter).to_df()


        # if format == "odb":
        #     io = BytesIO()
        #     pyodc.encode_odb(result, io)
        #     io.seek(0)
        #     return StreamingResponse(
        #         io,
        #         media_type="application/octet-stream",
        #         headers={
        #             "Content-Disposition": "attachment; filename=data.odb"
        #         }
        #     )

        if format == "parquet":
            io = BytesIO()
            result.to_parquet(io)
            io.seek(0)
            return StreamingResponse(
                io,
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": "attachment; filename=data.parquet"
                }
            )

        if format == "json":
            return Response(content=result.to_json(orient="records", double_precision=15), media_type="application/json")
            return 

        if format == "csv":
            return StreamingResponse(
                BytesIO(result.to_csv().encode("utf-8")),
                media_type="text/csv",
                headers={
                    "Content-Disposition": "attachment; filename=data.csv"
                }
            )

        return dict(
            error=f"Unknown Accept mimetypes or format: {request.accept_mimetypes} {format}"
        ), 500

    except pyfdb.FDBException as e:
        raise HTTPException(status_code=500, detail=dict(
            message = "FDB retrieve failed",
            error = str(e),
        ))



if __name__ == "__main__":
    args = parser.parse_args()

    print(" * Loadng IonBeam configuration...")
    ionbeam = parse_globals(args.config_folder, environment = args.environment)

    for key in ["fdb_schema_path", "fdb_root", "metkit_language_template"]:
        value = getattr(ionbeam.globals, key, None)
        print(" ✅" if value and value.exists() else " ❌",  key, value)
        assert value and value.exists(), f"Missing {key} in ionbeam.globals"
    
    # Patch metkit to allow custom mars keys
    print(" * Installing metkit overlay...")
    install_metkit_overlays(
        ionbeam.globals.metkit_language_template, ionbeam.globals.custom_mars_keys
    )

    # Load in the fdb schema to allow sorting keys between the mars request and other arguments
    print(" * Loading data schema...")
    fdb_schema = FDBSchemaFile(ionbeam.globals.fdb_schema_path)

    # Configure the FDB client
    print(" * Loading FDB5 library...")
    fdb = pyfdb.FDB(config = dict(
                    engine = "toc",
                    schema = str(ionbeam.globals.fdb_schema_path),
                    spaces = [
                        dict(
                            handler = "Default",
                            roots = [
                                dict(
                                    path = str(ionbeam.globals.fdb_root)
                                )
                            ]
                        )
                    ]
                ))
    
    uvicorn.run("ionbeam.api:app", host="0.0.0.0", port=8000, reload=True)