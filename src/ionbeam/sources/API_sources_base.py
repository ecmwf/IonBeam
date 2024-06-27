import dataclasses
from ..core.bases import Message, Source,  InputColumn
from pathlib import Path
from typing import Iterable, Any
import logging
from datetime import datetime
import requests
from urllib.parse import urljoin
import pickle

from ..metadata.db import IngestedChunk
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, URL

logger = logging.getLogger(__name__)

class API_Error(Exception): pass

@dataclasses.dataclass
class APISource(Source):
    """
    A base class to contain generic logic related to sources that periodically query API endpoints.


    .generate() is defined by the this base class, derived classes should define the remaining methods:
        - get_chunks
        - download_chunk 
    """

    "The time interval to ingest, can be overridden by globals.source_timespan"
    finish_after: int | None = None

    "A list of data to extract from the incoming API responses and add in as columns in the data"
    copy_metadata_to_columns: list[InputColumn] = dataclasses.field(default_factory = list)

    cache_version: int = 1 # increment this to invalidate the cache
    use_cache: bool = True

    def init(self, globals):
        super().init(globals)
        self.start_date, self.end_date = self.globals.ingestion_time_constants.query_timespan
        self.cache_directory = self.resolve_path(self.cache_directory, type="data")

        self.sql_engine = create_engine(URL.create(
            "postgresql+psycopg2",
            **self.globals.secrets["postgres_database"],
        ), echo = False)

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
    
    def load_data_from_cache(self, cache_key : str) -> Any:
        path = Path(self.cache_directory) / cache_key
        if not self.use_cache: raise KeyError("Cache is disabled")
        if not path.exists(): raise KeyError(f"Cache file {path} does not exist")

        try:
            with open(path, "rb") as f:
                d = pickle.load(f)
        except Exception as e:
            logger.debug(f"Cache file {path} could not be read: {e}")
            raise KeyError

        if d["version"] != self.cache_version: 
            logger.debug(f"Cache file {path} has version {d['version']} but we are expecting {self.cache_version}")
            raise KeyError
        
        if d["cache_key"] != cache_key:
            logger.debug(f"Cache file {path} has cache_key {d['cache_key']} but we are expecting {cache_key}")
            raise KeyError
        
        return d["data"]
        
    def save_data_to_cache(self, data : Any, cache_key : str):
        path = Path(self.cache_directory) / cache_key        
        with open(path, "wb") as f:
            pickle.dump(dict(
                version = self.cache_version,
                data = data,
                cache_key = cache_key,
                ), f)

    def query_chunk_ingested(self, cache_key : str) -> bool:
        """
        Query the database to see if this chunk has already been ingested.
        """
        with Session(self.sql_engine) as session:
            return session.query(IngestedChunk).where(
                IngestedChunk.source == self.metadata.source,
                IngestedChunk.cache_key == cache_key,
            ).one_or_none()
        
        
    def generate(self) -> Iterable[Message]:
        emitted_messages = 0

        for chunk in self.get_chunks(self.start_date, self.end_date):
            cache_key = chunk["key"]

            # Check if this chunk has already been ingested
            # by querying the database
            # if self.query_chunk_ingested(cache_key):
            #     logger.debug(f"Skipping {cache_key} as it's already been ingested.")
            #     continue

            logger.debug(f"Downloading {cache_key} from cache.")
            try:
                messages = self.download_chunk(chunk)
            except API_Error as e:
                logger.warning(f"{self.__class__.__name__}.get_chunks failed for {chunk = }\n{e}")
                continue

            # Yield the messages until we reach the limit or the end of the data
            for message in messages:
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
    