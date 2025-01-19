[x] Add ingestion time to ingestion chunk data structure
[x] add "did you mean" suggestions to subclass not found error message
[x] Add logic to back off if there has been a 429 error in the last x minutes
[x] Get the downstream acronet working
[x] test ingested acronet data against existing data 
[x] Fix fdb overwriting for acronet

[x] Add logic to download the most recent chunk first then try older ones 
[x] Add ability for source to modify the time_span of a chunk to only include the data recieved
[x] Add a flag that a chunk is empty but was completed correctly

[x] port meteotracker to new ingestion system
on system

[x] debug hang on SimpleODCEncoder 
[x] write class AddMeteotrackerMetadata(Parser):
[x] Debug why meteotracker is emitting so many message, probably not filtering correctly
[x] Allow canonicalise to accept a list of RawVariables
[x] modify canonicalise to add on the raw variable in this case
[x] fix location_feature = from_wkt(self.location_feature)
[x] figure out why the aronet data is now all nones
[x] Rewrite SCK to give the data out in one big chunk then use the infra I made for acronet
[x] Split out functionality to create RawVariable columns into separate action
[x] Sort out unit conversions for SCK because columns with the same name can have different units
[x] Deal with ownership of data. 
    [x] Allow actions to assume they own and can mutate input data
    [x] Modify the processor to give ownership over a message to the first consumer and a copy to all the rest.
[x] Convert the other two sources to also generate columns of raw Variables?

[x] Maybe collate acronet into one file after all?
[x] Rewrite metadata adding to use an upsert and share more between sources
[x] Refactor (remove) generate_metadata and tag_message
[x] swap out sqlalchemy json serialiser for orjson which supports datetimes and numpy arrays
[x] change JSON to JSONB in the db
[x] same for MT 
    [x] make datetime index
    [x] add external station id
    [x] use separate raw variable action

[x] When errors are suppressed save them so they can be debugged later with
```python
from ionbeam.core.singleprocess_pipeline import load_most_recent_error
saved_err = load_most_recent_error(config.globals)
```

[x] get downstream mt working
[x] port SCK to new ingestion system
    [ ] Try to triger a 429 and check for Retry-After header
    [ ] Add logic to exponentially back off if 429s keep happening


[ ]  figure out this ['chunk_date',                                   
                    'chunk_time'] in                                                
                    msg.metadata.columns but not in                                 
                    msg.data.columns   error

[ ] Deploy download cron jobs to server
[ ] Fix rest api to work with new format
    [ ] add an endpoint to directly get station data saving the filter step?

[ ] Make it even harder to nuke the ingestion data
[ ] Add way to nuke just station metadata for one source


pyarrow.lib.ArrowInvalid: Could not open Parquet input source '<Buffer>': Parquet file size is 0 bytes

[ ] take a look at concatenating parquet files




[ ] Strip out the concept of metadata entirely and make it all data?

match up the column names
{'Light', 'PN1.0', 'PM 1', 'Air Temperature', 'battery', 'PN10.0', 'Relative Humidity', 'Barometric Pressure', 'TVOC', 'CO2', 'PN5.0', 'PN0.5', 'PM 10', 'eCO2', 'PM 2.5', 'PN0.3', 'PN2.5', 'Noise Level'} != {'pm_avg_10', 'bat', 'bar', 'lat', 'pm_avg_2.5', 'h', 'ext_t', 'station_id', 'ext_h', 'light', 't', 'pm_avg_1', 'author', 'author_uuid', 'eco2', 'location_exposure', 'co2', 'tvoc', 'time', 'lon', 'station_name', 'noise_dba'}





[ ] Allow parsing ingestion times as a cmd line argumennt
[ ] Add arguments to nuke just one source from the db
[ ] Add arguments to reingest all data but keep db



[ ] Fix this code in codc 
```
if dtype == STRING:
    return_arr = return_arr.astype("|S{}".format(max(8, 8 * (1 + ((max(len(s) for s in arr) - 1) // 8)))))
```
it doesn't support unicode bytes.


[ ] pyodc does it actually do deduplication?
[ ] pyodc support deletion
[ ] update pyodc to take path : typing.Union[str, bytes, os.PathLike] and call os.fspath(path) see https://peps.python.org/pep-0519/