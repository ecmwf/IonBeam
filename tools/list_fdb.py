import pyfdb
from pathlib import Path
from ionbeam.core.config_parser import parse_single_action
from pydantic.dataclasses import dataclass

# ionbeam = parse_globals(Path("../config"), environment = "ewc")

# fdb = pyfdb.FDB(config = dict(
#                 engine = "toc",
#                 schema = ionbeam.globals.fdb_schema_path
#                 spaces = [
#                     dict(
#                         handler = "Default",
#                         roots = [
#                             dict(
#                                 path = str(ionbeam.globals.fdb_root)
#                             )
#                         ]
#                     )
#                 ]
#             ))

config, fdb_writer = parse_single_action(
    config_dir = Path(__file__).parents[1] / "config", 
    action_input = {"class" : "FDBWriter"},
    environment = "ewc"
)

fdb = fdb_writer.fdb

request = {'class': 'rd',
 'expver': 'xxxx',
 'stream': 'lwda',
 'aggregation_type': 'chunked',
 'date': '20241213',
 'platform': 'acronet',
 'internal_id': '49bb636e5d92f099'
 }

@dataclass
class ListResult():
    path: Path
    length: int
    offset: int
    keys: dict[str, str]

for l in fdb.list(request, keys= True):
    l = ListResult(**l)
    print(l.keys["date"], l.keys["time"])

