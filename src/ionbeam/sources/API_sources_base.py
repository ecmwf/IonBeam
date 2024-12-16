import dataclasses
import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from ..core.bases import InputColumns, Message, Source

logger = logging.getLogger(__name__)

class API_Error(Exception): pass

def recursive_get(dictionary, query):
    if dictionary is None or len(query) == 0: return dictionary
    first, *rest = query
    new_dictionary = dictionary.get(first, None)
    return recursive_get(new_dictionary, rest)

@dataclasses.dataclass
class APISource(Source):
    """
    A base class to contain generic logic related to sources that periodically query API endpoints.


    .generate() is defined by the this base class, derived classes should define the remaining methods:
        - get_chunks
        - download_chunk 
    """
    mappings: InputColumns

    "The time interval to ingest, can be overridden by globals.source_timespan"
    finish_after: int | None = None

    "A list of data to extract from the incoming API responses and add in as columns in the data"
    copy_metadata_to_columns: list[str] = dataclasses.field(default_factory = list)
    

    cache_version: int = 3 # increment this to invalidate the cache
    use_cache: bool = True

    def init(self, globals):
        super().init(globals)
        self.start_date, self.end_date = self.globals.ingestion_time_constants.query_timespan
        self.cache_directory = globals.cache_path / self.source
        self.mappings_dict = {column.name: column for column in self.mappings}

    def get_chunks(self, start_date : datetime, end_date: datetime) -> Iterable[Any]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        The return type can be an iterable of anything, eg strings representing session IDs
        It could also be the raw data if the API is structured that way.
        """
        raise NotImplementedError

    def download_chunk(self, chunk : Any) -> Iterable[Message]:
        """
        Take a chunk generated by get_chunks and download it to produce a message or messages.
        """
        raise NotImplementedError
    
    def load_data_from_cache(self, chunk=None, path: Path | None = None) -> Any:
        if chunk and path: raise ValueError("Only one of chunk or path should be provided")
        if not chunk and not path: raise ValueError("Either chunk or path should be provided")
        if chunk: path = Path(self.cache_directory) / chunk["key"]
        assert path is not None

        if not self.use_cache: raise KeyError("Cache is disabled")
        if not self.globals.overwrite_cache: raise KeyError("Cache is globally disabled")
        if not path.exists(): raise KeyError(f"Cache file {path} does not exist")

        try:
            with open(path, "rb") as f:
                d = pickle.load(f)
        except Exception as e:
            logger.debug(f"Cache file {path} could not be read: {e}")
            raise KeyError

        if not isinstance(d, dict) or "version" not in d:
            logger.debug(f"Cache file {path} is not in the expected format")
            raise KeyError
        
        if d["version"] != self.cache_version: 
            logger.debug(f"Cache file {path} has version {d['version']} but we are expecting {self.cache_version}")
            raise KeyError
        
        if chunk and d["cache_key"] != chunk["key"]:
            logger.debug(f"Cache file {path} has cache_key {d['cache_key']} but we are expecting {chunk['key']}")
            raise KeyError
        
        return d["chunk"], d["data"]
        
    def save_data_to_cache(self, chunk : dict, data : Any):
        path = Path(self.cache_directory) / chunk["key"]
        path.parent.mkdir(parents = True, exist_ok = True)        
        with open(path, "wb") as f:
            pickle.dump(dict(
                version = self.cache_version,
                chunk = chunk,
                data = data,
                cache_key = chunk["key"],
                ), f)

    # def query_chunk_ingested(self, cache_key : str) -> bool:
    #     """
    #     Query the database to see if this chunk has already been ingested.
    #     """
    #     with Session(self.sql_engine) as session:
    #         return session.query(IngestedChunk).where(
    #             IngestedChunk.source == self.metadata.source,
    #             IngestedChunk.cache_key == cache_key,
    #         ).one_or_none()
        
    def offline_chunks(self):
        """
        Return an iterable of chunks that have already been ingested
        """
        for path in self.cache_directory.glob("*.pickle"):
            try:
                chunk, _ = self.load_data_from_cache(path=path)
            except KeyError:
                continue
            yield chunk

    def generate(self) -> Iterable[Message]:
        emitted_messages = 0

        if self.globals.offline:
            chunk_iterable = self.offline_chunks()
        else:
            chunk_iterable = self.get_chunks(self.start_date, self.end_date, self.globals.ingestion_time_constants.granularity)


        for chunk in chunk_iterable:
            try:
                messages = self.download_chunk(chunk)
            except API_Error as e:
                logger.warning(f"{self.__class__.__name__}.get_chunks failed for {chunk = }\n{e}")
                continue

            # Yield the messages until we reach the limit or the end of the data
            for message in messages:
                for column_name in self.copy_metadata_to_columns:
                    column = self.mappings_dict[column_name]
                    message.data[column.name] = recursive_get(message.metadata.unstructured, column.key.split(".")) 

                yield message
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return
                
@dataclasses.dataclass
class RESTSource(APISource):
    endpoint = "scheme://example.com/api/v1" # Override this in derived classes

    def init(self, globals):
        super().init(globals)
        self.session = requests.Session()

        # Add retry for HTTP requests
        retries = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[502, 503, 504],
            allowed_methods={'POST'},
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.mount('http://', HTTPAdapter(max_retries=retries))

    def get(self, url, *args, **kwargs):
        """
        Wrapper around a HTTP get request:
            - uses a persistent session
            - raises an exception if the request fails
            - 
            - parses result as JSON
            """
        r = self.session.get(urljoin(self.endpoint, url), *args, **kwargs)
        r.raise_for_status()
        return r.json()
    