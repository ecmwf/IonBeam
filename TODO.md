[x] Add ingestion time to ingestion chunk data structure
[x] add "did you mean" suggestions to subclass not found error message
[x] Add logic to back off if there has been a 429 error in the last x minutes
[x] Get the downstream acronet working
[ ] test ingested acronet data against existing data 

[ ] Add logic to download the most recent chunk first then try older ones 

[ ] port meteotracker to new ingestion system
[ ] get downstream mt working

[ ] port SCK to new ingestion system
[ ] Try to triger a 429 and check for Retry-After header
[ ] Add logic to exponentially back off if 429s keep happening
[ ] get downstream sck working

[ ] Allow parsing ingestion times as a cmd line argumennt

[ ] Refactor generate_metadata and tag_message
[ ] Rewrite encoders/odb.py

[ ] Fix this code in codc 
```
if dtype == STRING:
    return_arr = return_arr.astype("|S{}".format(max(8, 8 * (1 + ((max(len(s) for s in arr) - 1) // 8)))))
```
it doesn't support unicode bytes.