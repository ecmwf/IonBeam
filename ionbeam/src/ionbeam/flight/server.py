# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The public Arrow Flight endpoint — the one interface sources and exporters speak.

A thin *synchronous* adapter over the async :class:`IonbeamCore`. It runs the core
(and its in-process event bus) on a background asyncio loop and bridges each Flight
RPC to it.

* ``DoPut``   CMD ``{"op":"ingest", ...}``        — source streams observations in
* ``GetFlightInfo`` CMD ``{"op":"dataset_range", ...}`` — resolve current builds in a range
* ``DoGet``   ticket ``{"op":"dataset", ...}``    — stream a built dataset out
* ``DoExchange`` CMD ``{"op":"await_triggers"|"await_datasets", ...}`` — push subscriptions
* ``DoAction`` ``health_check`` | ``trigger_source``
"""

import asyncio
import json
import threading
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.flight as flight
import structlog
from ionbeam_client.models import IngestionMetadata
from ionbeam_client.schema_metadata import GEOGRAPHIC_CRS
from ionbeam_client.schemes import structural_errors, unit_warnings
from pydantic import ValidationError

from ionbeam.application.core import IonbeamCore

logger = structlog.get_logger(__name__)

_SENTINEL = object()
_INGEST_QUEUE_DEPTH = 8  # bounded → backpressure the client when the store lags

TRIGGER_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("start", pa.timestamp("us", tz="UTC")),
        ("end", pa.timestamp("us", tz="UTC")),
    ]
)
DATASET_EVENT_SCHEMA = pa.schema([("event", pa.string())])


class IonbeamFlightServer(flight.FlightServerBase):
    def __init__(self, location: str, core: IonbeamCore):
        super().__init__(location)
        self._core = core
        self._shutting_down = threading.Event()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    # --- async bridge ----------------------------------------------------
    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def spawn(self, coro):
        """Schedule a long-running coroutine (e.g. the core's lifecycle) on the
        server's loop — the same loop the event bus runs on."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _drain(self, async_gen):
        """Drive an async generator from this sync thread, yielding its items."""
        while True:
            try:
                fut: Future = asyncio.run_coroutine_threadsafe(
                    async_gen.__anext__(), self._loop
                )
                yield fut.result()
            except StopAsyncIteration:
                break

    def shutdown(self):
        # gRPC shutdown waits for in-flight RPCs, and subscription streams never end
        # on their own — signal the _pump loops first so they finish within one poll.
        # Only stop the loop after the drain, so no bridged call is left pending.
        self._shutting_down.set()
        super().shutdown()
        self._loop.call_soon_threadsafe(self._loop.stop)

    # --- ingest ----------------------------------------------------------
    def do_put(self, context, descriptor, reader, writer):
        cmd = _command(descriptor)
        if cmd.get("op") != "ingest":
            raise flight.FlightServerError(f"do_put expects op=ingest, got {cmd.get('op')!r}")
        if not cmd.get("start") or not cmd.get("end"):
            raise flight.FlightServerError("ingest requires start and end")
        metadata = IngestionMetadata.model_validate(cmd["metadata"])
        registered = self._run(self._core.registered_dataset(metadata.name))
        if registered is None:
            raise flight.FlightServerError(
                f"dataset '{metadata.name}' is not registered; call register_dataset first"
            )
        if metadata.schema_hash() != registered.schema_hash:
            raise flight.FlightServerError(
                f"dataset '{metadata.name}' command metadata hash mismatch: "
                f"expected registered hash {registered.schema_hash}, got {metadata.schema_hash()}"
            )

        ingestion_id = UUID(cmd["id"]) if cmd.get("id") else uuid4()
        start = _utc_timestamp(cmd["start"], "start")
        end = _utc_timestamp(cmd["end"], "end")

        try:
            rows = self._stream_ingest(ingestion_id, registered.metadata, start, end, reader)
        except Exception as exc:
            # Keep the error concise: a full traceback exceeds gRPC's metadata limit.
            raise flight.FlightServerError(f"ingest failed: {str(exc)[:300]}")
        writer.write(pa.py_buffer(json.dumps({"rows": rows}).encode("utf-8")))

    def _stream_ingest(self, ingestion_id, metadata, start, end, reader):
        """Bridge the synchronous gRPC reader to the async ingestion through a
        bounded queue: each batch is canonicalized+written as it arrives, and a full
        queue blocks this thread's reads — backpressuring the client, so a fast
        source can't outrun the store."""
        queue: "asyncio.Queue" = asyncio.Queue(maxsize=_INGEST_QUEUE_DEPTH)

        async def _batches():
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    return
                yield item

        consumer = self.spawn(
            self._core.ingest(ingestion_id, metadata, start, end, _batches())
        )

        rows = 0
        try:
            for chunk in reader:
                if chunk.data is not None:
                    self._put_backpressured(queue, chunk.data, consumer)
                    if consumer.done():  # consumer failed early — stop feeding
                        break
                    rows += chunk.data.num_rows
        except BaseException:
            # The upload died mid-stream: never commit a partial window.
            consumer.cancel()
            raise

        if not consumer.done():
            self._put_backpressured(queue, _SENTINEL, consumer)
        consumer.result()  # surfaces any ingest error to the client
        return rows

    def _put_backpressured(self, queue, item, consumer):
        """Enqueue for the consumer, blocking (backpressure) until there's room — but
        wake periodically so a dead consumer can't deadlock the reading thread."""
        future = asyncio.run_coroutine_threadsafe(queue.put(item), self._loop)
        while True:
            try:
                future.result(timeout=0.25)
                return
            except FuturesTimeout:
                if consumer.done():
                    future.cancel()
                    return

    # --- discovery + read ------------------------------------------------
    def get_flight_info(self, context, descriptor):
        cmd = _command(descriptor)
        start = _utc_timestamp(cmd["start"], "start")
        end = _utc_timestamp(cmd["end"], "end")
        if cmd.get("op") != "dataset_range":
            raise flight.FlightServerError("get_flight_info expects op=dataset_range")
        locations = self._run(self._core.current_builds(cmd["dataset"], start, end))
        if not locations:
            raise flight.FlightServerError("no builds in range")

        schema = self._core.dataset_schema(locations[0])
        ticket = flight.Ticket(
            json.dumps({"op": "dataset", "locations": locations}).encode("utf-8")
        )
        endpoint = flight.FlightEndpoint(ticket, [])
        return flight.FlightInfo(schema, descriptor, [endpoint], -1, -1)

    def do_get(self, context, ticket):
        payload = json.loads(ticket.ticket.decode("utf-8"))
        if payload.get("op") != "dataset":
            raise flight.FlightServerError("do_get expects op=dataset ticket")
        locations = payload["locations"]
        if not locations:
            raise flight.FlightServerError("empty dataset locations")
        for location in locations:
            # tickets are client input; a location must stay inside the store
            if location.startswith("/") or ".." in location.split("/"):
                raise flight.FlightServerError(f"invalid dataset location {location!r}")
        schema = self._core.dataset_schema(locations[0])

        def batches():
            for location in locations:
                yield from self._drain(self._core.open_dataset(location))

        return flight.GeneratorStream(schema, batches())

    # --- subscriptions ---------------------------------------------------
    def do_exchange(self, context, descriptor, reader, writer):
        cmd = _command(descriptor)
        op = cmd.get("op")
        if op == "await_triggers":
            self._stream_triggers(context, cmd["source_name"], reader, writer)
        elif op == "await_datasets":
            datasets = set(cmd["datasets"]) if cmd.get("datasets") else None
            self._stream_datasets(
                context, cmd["exporter_name"], datasets, reader, writer
            )
        else:
            raise flight.FlightServerError(f"do_exchange unknown op {op!r}")

    def _stream_triggers(self, context, source_name, reader, writer):
        subscription = self._run(self._core.subscribe_triggers(source_name))
        self._pump(
            context, subscription, reader, writer, TRIGGER_SCHEMA, self._trigger_batch
        )

    def _stream_datasets(self, context, exporter_name, datasets, reader, writer):
        subscription = self._run(self._core.subscribe_datasets(exporter_name, datasets))
        self._pump(
            context,
            subscription,
            reader,
            writer,
            DATASET_EVENT_SCHEMA,
            self._dataset_batch,
        )

    def _pump(self, context, subscription, reader, writer, schema, to_batch):
        """Poll a subscription and push each event as a one-row batch. The bounded
        wait lets us notice client cancellation (``is_cancelled``) and disconnects
        (failed writes), so the stream and its bus queue tear down — no pinned thread.

        The bus ack certifies *work completion*, not delivery: after pushing an
        event the pump blocks on the exchange's return channel for the client's
        ack — an app-metadata frame carrying the event id, sent only once the
        client's handler finished. A client that dies or errors mid-handler tears
        the stream down instead, so the event stays pending and the bus
        redelivers it (at-least-once; handlers are idempotent). An ack for the
        wrong id is a protocol breach and tears down likewise."""
        try:
            writer.begin(schema)
            while not (context.is_cancelled() or self._shutting_down.is_set()):
                event = self._run(subscription.next(1.0))
                if event is None:
                    continue
                try:
                    writer.write_batch(to_batch(event))
                    ack = self._read_ack(reader)  # raises on disconnect/cancel
                except Exception:
                    break
                if ack != str(event.id):
                    logger.error(
                        "Client acked a different event than was delivered; tearing down",
                        delivered=str(event.id),
                        acked=ack,
                    )
                    break
                self._run(subscription.ack())
        finally:
            self._run(subscription.close())

    @staticmethod
    def _read_ack(reader) -> str:
        while True:
            chunk = reader.read_chunk()
            if chunk.app_metadata is not None:
                return chunk.app_metadata.to_pybytes().decode()

    @staticmethod
    def _trigger_batch(command):
        return pa.record_batch(
            [
                pa.array([str(command.id)]),
                pa.array([command.start_time], type=TRIGGER_SCHEMA.field("start").type),
                pa.array([command.end_time], type=TRIGGER_SCHEMA.field("end").type),
            ],
            schema=TRIGGER_SCHEMA,
        )

    @staticmethod
    def _dataset_batch(event):
        return pa.record_batch(
            [pa.array([event.model_dump_json()])], schema=DATASET_EVENT_SCHEMA
        )

    # --- actions ---------------------------------------------------------
    def do_action(self, context, action):
        if action.type == "health_check":
            yield flight.Result(b"ok")
        elif action.type == "register_dataset":
            metadata = self._parse_registration(action)
            try:
                schema_hash = self._run(self._core.register_dataset(metadata))
            except Exception as exc:
                raise flight.FlightServerError(str(exc)) from exc
            yield flight.Result(json.dumps({"schema_hash": schema_hash}).encode("utf-8"))
        elif action.type == "trigger_source":
            spec = json.loads(action.body.to_pybytes().decode("utf-8"))
            self._run(
                self._core.trigger_source(
                    spec["source_name"],
                    _utc_timestamp(spec["start"], "start"),
                    _utc_timestamp(spec["end"], "end"),
                )
            )
            yield flight.Result(b"ok")
        else:
            raise flight.FlightServerError(f"Unknown action {action.type!r}")

    def _parse_registration(self, action) -> IngestionMetadata:
        raw = action.body.to_pybytes().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise flight.FlightServerError(f"invalid ingestion metadata JSON: {exc}") from exc

        try:
            metadata = IngestionMetadata.model_validate(payload)
        except ValidationError as exc:
            raise flight.FlightServerError(f"invalid ingestion metadata: {exc}") from exc

        # Coordinates whose values core interprets (geographic x/y, station
        # altitude) sit at the same tier as the time axis: an uninterpretable
        # unit is rejected here, before anything ingests under it.
        errors = structural_errors(metadata.dataset_schema)
        if errors:
            raise flight.FlightServerError(
                f"invalid ingestion metadata: {'; '.join(errors)}"
            )

        for column in (
            *metadata.dataset_schema.variables,
            *metadata.dataset_schema.coordinates,
        ):
            for warning in unit_warnings(column):
                logger.warning(
                    "Dataset declaration warning",
                    dataset=metadata.name,
                    column=column.name,
                    warning=warning,
                )

        # Axes in a CRS ionbeam does not interpret are stored and served, but
        # every geo product skips them.
        non_geographic = [
            coordinate.name
            for coordinate in metadata.dataset_schema.coordinates
            if coordinate.axis in ("x", "y")
            and coordinate.crs is not None
            and coordinate.crs.upper() not in GEOGRAPHIC_CRS
        ]
        if non_geographic:
            logger.warning(
                "Dataset declares x/y axes in a CRS ionbeam does not interpret; "
                "it will be stored and served, but geo products (GeoParquet, "
                "EDR, ODB geolocation) will skip it",
                dataset=metadata.name,
                coordinates=non_geographic,
            )
        return metadata

    def list_actions(self, context):
        return [
            flight.ActionType("health_check", "Liveness/readiness probe"),
            flight.ActionType("register_dataset", "Validate and register a dataset schema"),
            flight.ActionType("trigger_source", "Publish a source trigger window (ops/testing)"),
        ]


def _command(descriptor) -> dict:
    if not descriptor.command:
        raise flight.FlightServerError("expected a command descriptor")
    return json.loads(descriptor.command.decode("utf-8"))


def _utc_timestamp(value: str, field: str) -> datetime:
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        raise flight.FlightServerError(f"{field} must carry a UTC offset")
    return ts
