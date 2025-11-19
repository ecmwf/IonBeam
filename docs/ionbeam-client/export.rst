Data Export
===========

Export handlers consume processed datasets when Ionbeam completes time-windowed aggregations.

Basic Export Handler
--------------------

.. code-block:: python

   import asyncio
   from ionbeam_client import IonbeamClient, IonbeamClientConfig
   from ionbeam_client.models import DataSetAvailableEvent

   config = IonbeamClientConfig(
       amqp_url="amqp://localhost:5672/"
   )

   async def export_handler(event: DataSetAvailableEvent, batch_stream):
       """Process aggregated dataset batches.
       
       Args:
           event: Dataset metadata and time window
           batch_stream: Async iterator of Arrow RecordBatches
       """
       print(f"Dataset: {event.metadata.name}")
       print(f"Window: {event.start_time} to {event.end_time}")
       
       async for batch in batch_stream:
           df = batch.to_pandas()
           # Export to your target system
           await write_to_storage(df)

   client = IonbeamClient(config)
   client.register_export_handler(
       exporter_name="my_exporter",
       handler=export_handler
   )

   async def main():
       async with client:
           await asyncio.Event().wait()

   asyncio.run(main())

Filtered Export
---------------

Process only specific datasets:

.. code-block:: python

   client.register_export_handler(
       exporter_name="filtered_exporter",
       handler=export_handler,
       dataset_filter={"netatmo", "meteotracker"}  # Only these sources
   )

Streaming Export
----------------

For large datasets, process batches incrementally:

.. code-block:: python

   async def streaming_export(event: DataSetAvailableEvent, batch_stream):
       """Stream data without loading everything into memory."""
       output_path = f"./data/{event.metadata.name}.parquet"
       
       async for batch in batch_stream:
           # Process batch-by-batch
           df = batch.to_pandas()
           await write_batch(df, output_path)