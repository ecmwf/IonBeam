import dataclasses
import logging
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any, Iterable, Self
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import JSON, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Mapped, Session, load_only, mapped_column
from sqlalchemy_utils import UUIDType
from urllib3.util import Retry

from ..core.bases import Mappings, Source, TabularMessage
from ..core.time import (
    TimeSpan,
)
from ..metadata.db import Base

logger = logging.getLogger(__name__)

class APISourcesBaseException(Exception): pass

class API_Error(APISourcesBaseException): pass
class ExistingDataException(APISourcesBaseException): pass

class RateLimitedException(APISourcesBaseException):
    """
    e.g 429 Too Many Requests
    The downstream api may also choose to raise this in other cases if rate limiting is detected.
    Provide a retry_after datetime to indicate when the rate limit will be lifted if possible.
    """
    retry_at: datetime
    def __init__(self, retry_at: datetime | None = None, 
                 response: requests.Response | None = None):
        self.response = response
        
        if retry_at is not None:
            self.retry_at = retry_at

        elif response is not None and "Retry-After" in response.headers:
            retry_after = int(response.headers["Retry-After"])
            self.retry_at = start_time + timedelta(seconds=retry_after)
        
        else:
            self.retry_at = start_time + timedelta(minutes=5)

@dataclasses.dataclass
class DataChunk:
    """
    Represents a chunk of a stream of data that has been downloaded from an API and stored in the db
    source: a string that uniquely represents the source API
    key: a string that unqiuely represents this stream of data within the API
    version: an integer that represents the version of the data
    time_span: the time range of the data
    ingestion_time: when we retrieved or attempted to retrieve the data from the API
    json: any metadata for the chunk that can't fit in the dataframe or the error message
    data: a dataframe of the data

    sucess: false if an error occured while trying to download the chunk
    empty: true if the download scuceeded but the chunk was empty anyway
    """
    source: str
    key: str
    version: int
    time_span: TimeSpan
    ingestion_time: datetime = dataclasses.field(default_factory=datetime.now)
    json: dict = dataclasses.field(repr=True, default_factory=dict)
    data: pd.DataFrame | None = dataclasses.field(repr=False, default=None)
    success: bool = True
    empty: bool = False

    @classmethod
    def make_error_chunk(cls, data_stream : "DataStream", time_span : TimeSpan, error : Exception, json = {}) -> Self:
        json = json.copy()
        json.update({
            "error": repr(error),
        })
        if isinstance(error, requests.exceptions.HTTPError):
            json.update({
                "status_code": error.response.status_code,
                "response_text": error.response.text,
            })
            if error.response.status_code == 429:
                error = RateLimitedException(response = error.response)
                json["retry_at"] = error.retry_at.isoformat()
        
        return cls(
                source=data_stream.source,
                key=data_stream.key,
                version=data_stream.version,
                time_span=time_span,
                success=False,
                empty=True,
                json = json,
                data=None,
            )


    def get_overlapping(self, db_session) -> "Iterable[DBDataChunk]":
        chunks = db_session.query(DBDataChunk).filter_by(
            source=self.source,
            key=self.key,
            version=self.version,
            ).filter(
                DBDataChunk.start_datetime < self.time_span.end,
                self.time_span.start < DBDataChunk.end_datetime).all()
        return chunks

    def write_to_db(self, db_session: Session, delete_previous_tries=True):
        """Save to the database"""
        overlapping = self.get_overlapping(db_session)
        for chunk in overlapping:
            if chunk.success:
                raise ExistingDataException(f"Trying to ingest data chunk {self} over {chunk.to_data_chunk()}")
            else:
                if delete_previous_tries:
                    db_session.delete(chunk)
                

        chunk = DBDataChunk(
            source=self.source,
            key=self.key,
            version=self.version,
            start_datetime=self.time_span.start,
            end_datetime=self.time_span.end,
            ingestion_time=self.ingestion_time,
            success = self.success,
            empty = self.empty,
            json = self.json,
            data=self.data.to_parquet() if self.data is not None else None
        )
        db_session.add(chunk)
        db_session.commit()

# This is just a flattened version of DataChunk for storage in the SQL database
class DBDataChunk(Base):
    __tablename__ = "ingested_chunk"
    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    source: Mapped[str] # Same as DataChunk.source
    key: Mapped[str] # Same as DataChunk.key
    version: Mapped[int] # Same as DataChunk.version
    start_datetime: Mapped[datetime] # Same as DataChunk.time_span.start
    end_datetime: Mapped[datetime] # Same as DataChunk.time_span.end
    ingestion_time: Mapped[datetime] # Same as DataChunk.ingestion_time
    success: Mapped[bool]
    empty: Mapped[bool]
    json: Mapped[dict] = mapped_column(JSON, nullable=True) # Same as DataChunk.json
    data: Mapped[bytes] = mapped_column(nullable=True) # == DataChunk.data.toparquet()

    __table_args__ = (
        # Index for fast lookups on source, key, version
        Index("ix_source_key_version", "source", "key", "version"),
        # Composite index for time range queries
        Index("ix_start_end_datetime", "start_datetime", "end_datetime"),
    )

    def to_data_chunk(self, only_metadata = False) -> DataChunk:
            return DataChunk(
                source=self.source,
                key=self.key,
                version=self.version,
                time_span=TimeSpan(self.start_datetime.replace(tzinfo=UTC), self.end_datetime.replace(tzinfo=UTC)),
                ingestion_time=self.ingestion_time.replace(tzinfo=UTC),
                success=self.success,
                empty = self.empty,

                # These are only loaded if only_metadata is False
                json=self.json if not only_metadata else {},
                data=pd.read_parquet(BytesIO(self.data)) if not only_metadata and self.data is not None else None
            )

@dataclasses.dataclass(frozen=True)
class DataStream:
    """
    Represents a logical stream of data from an API with no time component. 
    This could be a station, sensor or just the entire datasource as a single stream.
    source: a string that uniquely represents the source API
    key: a string that unqiuely represents this stream of data within the API
    version: an integer that represents the version of the data
    data: any additional information that the API needs to download the data

    It is left to the API implementation to decide how to structure key and day.
    """
    source: str
    key: str
    version: int
    data: Any = dataclasses.field(hash=False)
    last_ingestion: datetime | None = None

    def query_db(self, db_session: Session, time_span : TimeSpan,
                success: bool | None = None,
                ingested_after : datetime | None = None,
                non_empty : bool = False):
        
        query = db_session.query(DBDataChunk).filter_by(
            source=self.source,
            key=self.key,
            version=self.version,
            ).filter(
                DBDataChunk.start_datetime <= time_span.end,
                time_span.start <= DBDataChunk.end_datetime
            )
        if success is not None:
            query = query.filter(DBDataChunk.success == success)
        if ingested_after is not None:
            query = query.filter(DBDataChunk.ingestion_time > ingested_after)
        if non_empty:
            query = query.filter(DBDataChunk.empty == False)
        return query

    def get_timespans_from_db(self, db_session: Session, 
                            time_span : TimeSpan,
                            success: bool | None = None,
                            ingested_after : datetime | None = None,
                            non_empty : bool = False) -> Iterable[TimeSpan]:
        
        filtered_query = self.query_db(db_session, time_span, success, ingested_after)
        column_query = filtered_query.with_entities(DBDataChunk.start_datetime, DBDataChunk.end_datetime)
        for start, end in column_query.all():
            yield TimeSpan(start.replace(tzinfo=UTC), end.replace(tzinfo=UTC))

    def get_chunks_from_db(self, db_session: Session, 
                           time_span : TimeSpan,
                            success: bool | None = None,
                            ingested_after : datetime | None = None,
                            non_empty : bool = False) -> Iterable[DataChunk]:
        """Read the data from the database cache"""
        filtered_query = self.query_db(db_session, time_span, success, ingested_after)
        for db_chunk in filtered_query.all():
            yield db_chunk.to_data_chunk()

    def get_chunk_metadata_from_db(self, db_session: Session, 
                            time_span : TimeSpan,
                            success: bool | None = None,
                            ingested_after : datetime | None = None,
                            non_empty : bool = False) -> Iterable[DataChunk]:
        filtered_query = self.query_db(db_session, time_span, success, ingested_after)
        filtered_query = filtered_query.options(load_only(
            DBDataChunk.source,
            DBDataChunk.key,
            DBDataChunk.version,
            DBDataChunk.start_datetime,
            DBDataChunk.end_datetime,
            DBDataChunk.ingestion_time,
            DBDataChunk.success,
            DBDataChunk.empty,
            raiseload=True # Raises an exeption if later code tries to load the data columns
            ))
        
        for db_chunk in filtered_query.all():
            yield db_chunk.to_data_chunk(only_metadata =True)

    def write_to_db(self, db_session, last_ingestion_time: datetime | None = None):
        """Save to the database"""
        # Perform an upsert, insert if (source, key, version) doesn't exist
        # otherwise update the existing row

        values_dict = dict(
            source=self.source,
            key=self.key,
            version=self.version,
            last_ingestion_time = datetime.now(UTC) if last_ingestion_time is None else last_ingestion_time
        )

        stmt = insert(DBDataStream).values(**values_dict)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "key", "version"],
            set_={"last_ingestion_time": values_dict["last_ingestion_time"]}
        )

        db_session.execute(stmt)
        db_session.commit()

    def get_last_ingestion_time(self, db_session: Session) -> datetime | None:
        db_data_stream = db_session.query(DBDataStream).filter_by(
            source=self.source,
            key=self.key,
            version=self.version,
        ).one_or_none()

        if db_data_stream is None:
            return None
        return db_data_stream.last_ingestion_time

# Database information per stream that is used to keep track of ingestion
class DBDataStream(Base):
    __tablename__ = "ingested_stream"
    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    source: Mapped[str]
    key: Mapped[str]
    version: Mapped[int]

    # Keep track of when we last looked at the database
    # Any DataChunk with ingestion_time > last_ingestion_time should be processed
    # into the pipeline.
    # This allows us to pick up updates to older date
    last_ingestion_time: Mapped[datetime] # Same as DataChunk.ingestion_time

    __table_args__ = (
        # Index for fast lookups on source, key
        Index("ix_source_key", "source", "key"),
        # Composite index for time range queries
        Index("ix_last_ingestion_time", "last_ingestion_time"),
        # Force this trio to be unique so that we can trigger upserts on it
        UniqueConstraint('source', 'key', 'version', name='uix_source_key_version'),
    )

    def to_data_stream(self) -> DataStream:
        return DataStream(
            source=self.source,
            key=self.key,
            version=self.version,
            data=None
        )

class AbstractDataSourceMixin(ABC):
    """
    A mixin class that defines the interface for a data source that downloads data from an API
    """
    source: str
    cache_version: int
    maximum_request_size = timedelta(days=10)

    @abstractmethod
    def get_data_streams(self, time_span: TimeSpan) -> Iterable[DataStream]:
        """
        Ask the API what datastreams exist for this time range
        """
        pass
    
    @abstractmethod
    def download_chunk(self, data_stream: DataStream, time_span: TimeSpan) -> DataChunk:
        """
        Download a chunk of data from the API
        May have return a chunk with a smaller time_span than the one requested (but must be inside it)
        """
        pass

    @abstractmethod
    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_span: TimeSpan) -> Iterable[TabularMessage]:
        """
        Emit messages corresponding to the data downloaded from the API in this time_span
        This is separate from download_data_stream_chunks to allow for more complex processing.
        This could for example involve merging data from multiple data streams.
        This happens for the Acronet API where we can download data for each sensor class
        but we want to emit messages for each station. So this method regroups the messages.
        """
        pass

@dataclasses.dataclass
class APISource(Source, AbstractDataSourceMixin):
    """
    The generic logic related to sources that periodically query API endpoints.
    This includes:
        - Organsing data into logical streams
        - Downloading the data in each stream in time based chunks
        - Storing those chunks in a database
        - Retrying chunks if some chunks fail on the first attempt.
        - Recombining chunks to form a complete dataset.
    """
    mappings: Mappings

    "The time interval to ingest, can be overridden by globals.source_timespan"
    finish_after: int | None = None
    

    cache_version: int = 3 # increment this to invalidate the cache
    use_cache: bool = True
    source: str = "should be set by derived class"
    maximum_request_size: timedelta = timedelta(days=1)
    minimum_request_size: timedelta = timedelta(minutes=5)
    max_time_downloading: timedelta = timedelta(seconds = 30)

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)

    def check_source_for_errors(self, db_session : Session, since: datetime) -> datetime:
        error_chunks = db_session.query(DBDataChunk).filter_by(
            source=self.source).filter(
                DBDataChunk.ingestion_time > since,
                DBDataChunk.success == False
            ).all()
        

        retry_at = datetime.now(UTC) - timedelta(seconds=1)
        for chunk in error_chunks:
            if chunk.json.get("status_code") == 429:
                # if the response has a retry after header, use that
                # otherwise use the ingestion time + 5 minutes
                if "retry_at" in chunk.json:
                    retry_at = max(datetime.fromisoformat(chunk.json["retry_at"]), retry_at)
                else :
                    retry_at = max(chunk.ingestion_time.replace(tzinfo=UTC) + timedelta(minutes=5), retry_at)
            

        return retry_at


    def gaps_in_database(self, db_session: Session, data_stream: DataStream, time_span : TimeSpan
                     ) -> list[TimeSpan]:
        """Given a DataStream and a time span, return any gaps in the time span that need to be downloaded"""
        
        # Get all the timespan of all chunks that have been ingested sucessfully
        chunks_time_spans =  list(data_stream.get_timespans_from_db(db_session, time_span, success=True, non_empty=False))
        chunks_to_ingest = time_span.remove(chunks_time_spans)
        split_chunks = [
            c 
            for chunk in chunks_to_ingest
            for c in chunk.split(self.maximum_request_size)
        ]

        # Remove the last chunk if
        # * there are any
        # * it's too close to recent side of the requested time interval
        # * it's too small
        if split_chunks \
            and (split_chunks[-1].start + self.minimum_request_size) > time_span.end \
            and split_chunks[-1].delta() < self.minimum_request_size:
            logger.debug(f"Removing last chunk because its time interval is too small. ({split_chunks[-1].delta()}")
            split_chunks = split_chunks[:-1]

        return split_chunks

    def fail(self, error):
        if isinstance(error, Exception): raise error
        else: raise Exception("Forced failure")

    def download_data(self, data_streams: Iterable[DataStream], 
                      query_time_span, 
                      fail = False) -> Iterable[DataChunk]:
        
        start_time = datetime.now(UTC)
        
        with Session(self.globals.sql_engine) as db_session:
            # Check if the source has been rate limited recently
            retry_at = self.check_source_for_errors(db_session, since = start_time - timedelta(days=1))
            if retry_at > start_time:
                logger.info(f"Source {self.source} was rate limited, waiting until {retry_at} ({retry_at - start_time})")
                return
        
            # Get the list of queries we need to make
            gaps_by_datastream = {data_stream : reversed(self.gaps_in_database(db_session, data_stream, query_time_span))
                                  for data_stream in data_streams}

            # Download any data we need for each data stream
            # 
            while datetime.now(UTC) - start_time < self.max_time_downloading:
                for data_stream, gaps in gaps_by_datastream.items():
                    gap = next(gaps, None)
                    if gap is None: 
                        continue

                    # Begin a nested db session
                    # This lets us rollback just this bit (automatically)
                    # if an unexpected failure occurs in here
                    with db_session.begin_nested():
                        try:
                            if fail: self.fail(fail)
                            data_chunk = self.download_chunk(data_stream, gap)
                            data_chunk.write_to_db(db_session)
                            logger.debug(f"Downloaded data and wrote to db for {data_stream.key} {gap.delta()}")
                            yield data_chunk
                        
                        except ExistingDataException:
                            logger.debug(f"Data already exists in db for {data_stream.key} {gap.delta()}")
                            if self.globals.die_on_error: raise

                        except Exception as e:
                            logger.warning(f"Failed to download chunk {data_stream.key} {gap.delta()} {e}")
                            error_chunk = DataChunk.make_error_chunk(data_stream, gap, e)
                            error_chunk.write_to_db(db_session)
                            if self.globals.die_on_error: raise
                            yield error_chunk

                            # Completely give up for now if we are rate limited
                            if isinstance(e, RateLimitedException):
                                return

    def generate(self) -> Iterable[TabularMessage]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        """
        query_time_span = self.globals.ingestion_time_constants.query_timespan
        granularity = self.globals.ingestion_time_constants.granularity

        data_streams = list(self.get_data_streams(query_time_span))
        logger.debug(f"Got data streams {data_streams}")

        _ = list(self.download_data(data_streams, query_time_span))

        # Let the API implementation do any additional processing
        # and emit the messages
        emitted_messages = 0


        # There's a bit of a tradeoff in the design here
        # When doing realtime ingestion we're ingesting in short 5 minute intervals
        # When doing historical ingestion it makes sense to do larger queries
        # But because we are emitting messages for each 5 minute output timespan
        # If the ingestion chunks are too large then we have to repeatedly load and filter them
        # The optimal stategy would be to emit by chunk when chunk_span >> granularity
        # and emit by timespan when chunk_span ~= granularity

        with Session(self.globals.sql_engine) as db_session:
                
            # Get all the chunks that were downloaded more recently than data_stream.last_ingestion_time
            # Only get the metadata for now
            new_or_updated_chunks = [
                data_chunk
                for data_stream in data_streams
                for data_chunk in data_stream.get_chunks_from_db(db_session, query_time_span, 
                                                                 ingested_after = data_stream.get_last_ingestion_time(db_session),
                                                                 non_empty=True) 
            ]
            logger.debug(f"Got new or updated chunks {len(new_or_updated_chunks)}")


            # Determine the list of output time spans that overlap one of the modified chunks
            # Also keep a reference to the chunk data for each of them
            affected_time_spans = defaultdict(list)
            for chunk in new_or_updated_chunks:
                for ts in chunk.time_span.split_rounded(granularity):
                    affected_time_spans[ts].append(chunk)

            logger.debug(f"Got affected time spans {len(affected_time_spans)}")


        logger.debug("Starting to emit messages")
        for ts, chunks in affected_time_spans.items():
            logger.debug(f"Got {len(chunks)} modified chunks for ts {ts}")

            # Go through and also load all the old chunks we need for each affected time span
            for data_stream in data_streams:
                for chunk in data_stream.get_chunks_from_db(db_session, ts, non_empty=True):
                    chunks.append(chunk)

            logger.debug(f"Got {len(chunks)} total chunks for ts {ts}")

            msgs = list(self.emit_messages(chunks, ts))

            logger.debug(f"Emitted {len(msgs)} messages for {ts}")

            for msg in msgs:
                yield msg
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return
        
        with Session(self.globals.sql_engine) as db_session:
            # Update the last_ingestion_time for each data stream
            for ds in data_streams:
                ds.write_to_db(db_session)


                
@dataclasses.dataclass
class RESTSource(APISource):
    endpoint = "scheme://example.com/api/v1" # Override this in derived classes

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
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


