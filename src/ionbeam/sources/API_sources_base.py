import dataclasses
import logging
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from time import time
from typing import Any, Iterable, Literal, Self
from urllib.parse import urljoin

import pandas as pd
import requests
from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import Mapped, Session, load_only, mapped_column
from sqlalchemy_utils import UUIDType

from ..core.bases import TabularMessage
from ..core.source import Source
from ..core.time import (
    TimeSpan,
)
from ..metadata.db import Base
from ..singleprocess_pipeline import fmt_time

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
            logger.debug(f"Retry-After header provided, delaying for {timedelta(seconds=retry_after)}")
            after = min(timedelta(seconds=retry_after), timedelta(minutes = 30))
            self.retry_at = datetime.now(UTC) + after
            
        
        else:
            logger.debug("No Retry-After header, delaying for 5 minutes.")
            self.retry_at = datetime.now(UTC) + timedelta(minutes=5)

@dataclass(eq=True, frozen=True)
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
    json: dict = dataclasses.field(repr=True, default_factory=dict, compare=False, hash=False)
    data: pd.DataFrame | None = dataclasses.field(repr=False, default=None, compare=False, hash=False)
    success: bool = True
    empty: bool = False

    def __repr__(self) -> str:
        return f"DataChunk({self.source}, {self.key}, {self.version}, {self.time_span}, {self.ingestion_time}, {self.success}, {self.empty})"

    @classmethod
    def make_empty_chunk(cls, data_stream: "DataStream", time_span) -> Self:
        return cls(
                source=data_stream.source,
                key = data_stream.key,
                version = data_stream.version,
                empty = True,
                time_span = time_span,
                json = {},
                data = None,
            )

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
    
    def load_data(self, db_session: Session):
        """Load the data from the database"""
        db_chunk = db_session.query(DBDataChunk).filter_by(
            source=self.source,
            key=self.key,
            version=self.version,
            start_datetime=self.time_span.start,
            end_datetime=self.time_span.end,
        ).one()
        return db_chunk.to_data_chunk()

    def write_to_db(self, db_session: Session, delete_previous_tries=True):
        """Save to the database"""
        overlapping = self.get_overlapping(db_session)
        for chunk in overlapping:
            if chunk.success and not chunk.empty:
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
    json: Mapped[dict] = mapped_column(JSONB, nullable = True) # Same as DataChunk.json
    # json: Mapped[dict] = mapped_column(mutable_json_type(dbtype=JSONB, nested=True), nullable = True) # Same as DataChunk.json
    data: Mapped[bytes] = mapped_column(nullable=True) # == DataChunk.data.toparquet()

    __table_args__ = (
        # Index for fast lookups on source, key, version
        Index("ix_source_key_version", "source", "key", "version"),

        # Composite index for time range queries
        Index("ix_start_end_datetime", "start_datetime", "end_datetime"),

        # Index for fast lookups on ingestion time
        Index("ix_ingestion_time", "ingestion_time"),
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

@dataclass(frozen=True)
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

    def __repr__(self) -> str:
        return f"DataStream({self.source}, {self.key}, {self.version}, {self.last_ingestion})"

    def get_chunks(self, db_session: Session, time_span : TimeSpan,
                mode: Literal["metadata", "data"] = "data",
                success: bool | None = None,
                ingested_after : datetime | None = None,
                ingested_before : datetime | None = None,

                # Empty chunks serve as a sentinel that we have queried for data from this timespan but there wasn't any
                # By default do not return empty chunks, but it's very important to set this to None when deciding 
                # what data to query for next
                empty : bool | None = False
                ) -> Iterable[DataChunk]:
        
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
        
        if ingested_before is not None:
            query = query.filter(DBDataChunk.ingestion_time < ingested_before)
        
        if empty is not None:
            query = query.filter(DBDataChunk.empty == empty)

        if mode == "metadata":
            query = query.options(load_only(
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
        
            for db_chunk in query.all():
                yield db_chunk.to_data_chunk(only_metadata =True)
        
        elif mode == "data":
            for db_chunk in query.all():
                yield db_chunk.to_data_chunk()

        else:
            raise ValueError(f"Invalid mode {mode}")




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
        UniqueConstraint('source', 'key', 'version', name='uix_datastream_source_key_version'),
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
    def affected_time_spans(self, chunk : DataChunk, granularity : timedelta) -> Iterable[TimeSpan]:
        """Override this when the data is very sparse"""
        pass
    
    @abstractmethod
    def download_chunk(self, data_stream: DataStream, time_span: TimeSpan) -> DataChunk:
        """
        Download a chunk of data from the API
        May have return a chunk with a smaller time_span than the one requested (but must be inside it)
        """
        pass

    @abstractmethod
    def emit_messages(self, relevant_chunks : Iterable[DataChunk], time_span_group: Iterable[TimeSpan]) -> Iterable[TabularMessage]:
        """
        Emit messages corresponding to the data downloaded from the API in this time_span
        This is separate from download_data_stream_chunks to allow for more complex processing.
        This could for example involve merging data from multiple data streams.
        This happens for the Acronet API where we can download data for each sensor class
        but we want to emit messages for each station. So this method regroups the messages.
        """
        pass

@dataclass
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

    "The time interval to ingest, can be overridden by globals.source_timespan"
    finish_after: int | None = None
    

    cache_version: int = 3 # increment this to invalidate the cache
    use_cache: bool = True
    source: str = "should be set by derived class"
    maximum_request_size: timedelta = field(kw_only=True)
    minimum_request_size: timedelta = field(kw_only=True)
    max_time_downloading: timedelta = field(kw_only=True)

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
        chunks_time_spans =  list(c.time_span for c in data_stream.get_chunks(db_session, time_span, success=True, mode="metadata", empty=None))
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
            # logger.debug(f"Removing last chunk because its time interval is too small. ({split_chunks[-1].delta()}")
            split_chunks = split_chunks[:-1]

        return split_chunks

    def fail(self, error):
        if isinstance(error, Exception): raise error
        else: raise Exception("Forced failure")

    def download_data(self, data_streams: Iterable[DataStream], 
                      query_time_span, 
                      fail = False) -> Iterable[DataChunk]:
        
        start_time = datetime.now(UTC)
        logger.info(f"Starting download for source {self.source}")
        logger.debug(f"{self.max_time_downloading = }, {self.maximum_request_size = }, {self.minimum_request_size = }")
        
        with self.globals.sql_session.begin() as db_session:
            # Check if the source has been rate limited recently
            retry_at = self.check_source_for_errors(db_session, since = start_time - timedelta(days=1))
            if retry_at > start_time:
                logger.info(f"Downloading from source {self.source} was previously rate limited, waiting {fmt_time((retry_at - start_time).total_seconds())} until trying again.")
                return
        
            # Get the list of queries we need to make
            gaps_by_datastream = {data_stream : list(self.gaps_in_database(db_session, data_stream, query_time_span))
                                  for data_stream in data_streams}

        # Download any data we need for each data stream
        while gaps_by_datastream:
            # Remove any that are done
            gaps_by_datastream = {data_stream : gaps for data_stream, gaps in gaps_by_datastream.items() if gaps}
            for data_stream, gaps in gaps_by_datastream.items():
                time_left = self.max_time_downloading - (datetime.now(UTC) - start_time)
                logger.info(f"{time_left.total_seconds():.0f} seconds left to download for  {data_stream.key}")
                if time_left < timedelta(0):
                    logger.info(f"Stopping downloads for {data_stream.key} after {self.max_time_downloading} seconds.")
                    return

                logger.info(f"Downloading data for datastream '{data_stream.key}', currently have {len(gaps)} gap(s) to process.")
                if not gaps: 
                    continue
                gap = gaps.pop()
                logger.info(f"Downloading data for time span {gap}")

                # Start a session here so that if anything goes wrong it gets automatically rolled back
                with self.globals.sql_session.begin() as db_session:
                    try:
                        if fail: self.fail(fail)
                        t0 = time()
                        data_chunk = self.download_chunk(data_stream, gap)
                        data_chunk.write_to_db(db_session)
                        logger.info(f"Downloaded data and wrote to db for stream {data_stream.key} in {fmt_time(time() - t0)} {data_chunk.empty = }")
                        yield data_chunk
                    
                    except ExistingDataException:
                        logger.warning(f"Data already exists in db for {data_stream.key} {gap.delta()}")
                        if self.globals.die_on_error: raise

                    except Exception as e:
                        logger.warning(f"Failed to download chunk {data_stream.key} {gap.delta()} {e}")
                        error_chunk = DataChunk.make_error_chunk(data_stream, gap, e)
                        error_chunk.write_to_db(db_session)
                        if self.globals.die_on_error: raise
                        yield error_chunk

                        # Completely give up for now if we are rate limited
                        if isinstance(e, RateLimitedException) or (isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429):
                            logger.warning("Rate limited, giving up for now")
                            # Todo: write a source level flag to indicate that we are rate limited
                            # And when we should next attempt this source
                            return
                    # return

    def get_all_data_streams(self, db_session: Session, timespan : TimeSpan | None = None) -> Iterable[DataStream]:
        for ds in db_session.query(DBDataStream).filter_by(source=self.source).all():
            yield ds.to_data_stream()

    def affected_time_spans(self, chunk : DataChunk, granularity : timedelta) -> Iterable[TimeSpan]:
        """Override this when the data is very sparse"""
        return chunk.time_span.split_rounded(granularity)


    def ingest_to_pipeline(self, data_streams: Iterable[DataStream]):
        # There's a bit of a tradeoff in the design here
        # When doing realtime ingestion we're ingesting in short 5 minute intervals
        # When doing historical ingestion it makes sense to do larger queries
        # It would be inefficient to hold 10 days worth of data in memory and then filter it for each 5 minute output timespan
        # So instead we pick one timespan and find all the chunks it needs
        # Then we find all the timespans that are affected by those chunks
        # We call this a timespan group, and we process all the chunks in the group before moving on to the next group
        # This way we can operate on large chunks efficiently

        emitted_messages = 0
        if self.globals.reingest_from is not None: 
            # Todo find a cleaner way to enforce this at the config level
            reingest_from = self.globals.reingest_from.replace(tzinfo = UTC)
            logger.debug(f"reingesting from {reingest_from}")
        else:
            reingest_from = None

        granularity = self.globals.ingestion_time_constants.granularity
        query_time_span = self.globals.ingestion_time_constants.query_timespan

        if reingest_from:
            query_time_span = dataclasses.replace(query_time_span, start = reingest_from)
        
        with self.globals.sql_session.begin() as db_session:

            new_chunks = [
                data_chunk
                for data_stream in self.get_all_data_streams(db_session)
                for data_chunk in data_stream.get_chunks(db_session, query_time_span, 
                                                                 # If reingest_from is set then ignore when the chunks were actually ingested 
                                                                 mode="metadata",
                                                                 ingested_after = None if reingest_from is not None else data_stream.get_last_ingestion_time(db_session),
                                                                 ) 
            ]
            logger.info(f"New or updated chunks {len(new_chunks)}")


            # Determine the list of output time spans that overlap one of the modified chunks
            # Create a set of new chunks for each time span
            affected_time_spans : dict[TimeSpan, set[DataChunk]] = defaultdict(set)
            for chunk in new_chunks:
                for ts in self.affected_time_spans(chunk, granularity):
                    affected_time_spans[ts].add(chunk)

            logger.info(f"Affected time spans {len(affected_time_spans)}")


        logger.debug("Starting to emit messages, looping over time spans")
        while affected_time_spans:
            
            # Get the next time span to work on
            current_time_span, chunk_group = affected_time_spans.popitem()
            logger.info(f"Picked the next time span to work on {current_time_span}")

            # When the chunks are large it makes sense to operate on many time spans at once
            # get all the timespans affected by all the chunks we have loaded anyway.
            # This is a heuristic but should define a nice unit of work to do.
            time_spans_affected_by_this_group = {current_time_span,} | set(
                t
                for chunk in chunk_group
                for t in self.affected_time_spans(chunk, granularity)
                if t in affected_time_spans # Don't process time spans we've already processed
                )
            
            for t in time_spans_affected_by_this_group:
                if t in affected_time_spans:
                    del affected_time_spans[t]

            logger.info(f"Expanded to group of {len(time_spans_affected_by_this_group)} time spans")

            # Go through and actually load the chunks
            chunk_group = set()
            for ts in time_spans_affected_by_this_group:
                logger.debug(f"Loading all data chunks for {ts}")
                for data_stream in data_streams:
                    for chunk in data_stream.get_chunks(db_session, ts, ingested_before = data_stream.get_last_ingestion_time(db_session), mode="metadata"):
                        # Avoid loading the same chunk multiple times
                        if chunk not in chunk_group:
                            chunk = chunk.load_data(db_session)
                            chunk_group.add(chunk)

            logger.debug(f"{len(chunk_group)} total chunks contribute to this timespan group, about {len(chunk_group) / len(data_streams):.1f} per datastream")

            msgs = list(self.emit_messages(chunk_group, time_spans_affected_by_this_group))

            if len(msgs) == 0:
                logger.info(f"Emitted {len(msgs)} messages for time span group")

            for msg in msgs:
                yield msg
                emitted_messages += 1
                if self.finish_after is not None and emitted_messages >= self.finish_after:
                    return
        
        # If we get here without an error then mark the chunks as having been ingested properly
        # unless we used the finish_after parameter to limit the number of messages processed
        if self.globals.finish_after is None:
            with self.globals.sql_session.begin() as db_session:
                # Update the last_ingestion_time for each data stream
                for ds in data_streams:
                    ds.write_to_db(db_session)

    def generate(self) -> Iterable[TabularMessage]:
        """
        Return an iterable of objects representing chunks of data we should download from the API
        """
        query_time_span = self.globals.ingestion_time_constants.query_timespan

        if self.globals.download:
            logger.info(f"Starting downloads for source {self.source}")
            # Ask the data source what data streams exist for this time span
            # This may require API requests
            try:
                data_streams = list(self.get_data_streams(query_time_span))
            except Exception as e:
                logger.warning(f"Failed to get data streams for {self.source} with error {e}")
                if self.globals.die_on_error: raise
                else: return

            with self.globals.sql_session.begin() as db_session:
                for ds in data_streams:
                    ds.write_to_db(db_session)

            # Download the data for each stream
            logger.info(f"For source {self.source}, got data streams {data_streams}")
            _ = list(self.download_data(data_streams, query_time_span))

        if self.globals.ingest_to_pipeline:
            logger.info(f"Starting to ingest to pipeline for source {self.source}")
            with self.globals.sql_session.begin() as db_session:
                data_streams = list(self.get_all_data_streams(db_session))
            yield from self.ingest_to_pipeline(data_streams)
        



                
@dataclass
class RESTSource(APISource):
    endpoint = "scheme://example.com/api/v1" # Override this in derived classes

    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.session = requests.Session()
        # self.session.timeout = 3

        # # Add retry for HTTP requests
        # retries = Retry(
        #     total=3,
        #     backoff_factor=0.1,
        #     status_forcelist=[502, 503, 504],
        #     respect_retry_after_header=True,
        #     allowed_methods=None, # Allow retry on any VERB not just idempotent ones
        # )

        # self.session.mount('https://', HTTPAdapter(max_retries=retries))
        # self.session.mount('http://', HTTPAdapter(max_retries=retries))

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


