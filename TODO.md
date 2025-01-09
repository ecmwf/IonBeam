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
[/] get downstream mt working
[x] debug hang on SimpleODCEncoder 
[ ] write class AddMeteotrackerMetadata(Parser):
[ ] Debug why meteotracker is emitting so many message, probably not filtering correctly

[ ] port SCK to new ingestion system
[ ] Try to triger a 429 and check for Retry-After header
[ ] Add logic to exponentially back off if 429s keep happening
[ ] get downstream sck working

[ ] Rewrite metadata adding to use an upsert and share more between sources

[ ] Allow parsing ingestion times as a cmd line argumennt

[ ] Refactor generate_metadata and tag_message
[ ] Rewrite encoders/odb.py

[ ] Fix this code in codc 
```
if dtype == STRING:
    return_arr = return_arr.astype("|S{}".format(max(8, 8 * (1 + ((max(len(s) for s in arr) - 1) // 8)))))
```
it doesn't support unicode bytes.


[ ] pyodc does it actually do deduplication?
[ ] pyodc support deletion
[ ] update pyodc to take path : typing.Union[str, bytes, os.PathLike] and call os.fspath(path) see https://peps.python.org/pep-0519/