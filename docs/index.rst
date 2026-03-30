Ionbeam
=====================

**IonBeam** is an event-driven platform for **stream-based processing of IoT observations**. It provides real-time collection, transformation, and distribution of environmental monitoring data from diverse IoT sources.
The platform implements a message-driven pipeline that:

* Ingests observational data from multiple sources such as REST APIs, MQTT brokers, and file servers
* Transforms and validates observations using standardized data models aligned with CF (Climate and Forecast) conventions
* Aggregates data into configurable time windows for efficient querying
* Exports processed data to multiple formats and systems simultaneously (e.g., ODB, GeoParquet)

Built on event-sourcing principles, IonBeam captures incoming observations as immutable events. These events are processed into specialized, query-optimized read models tailored to downstream use cases. This architecture decouples ingestion from export concerns, enabling independent scaling and evolution of each component.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture
   domain
   messaging-interface
   dataset-schema
   ionbeam-client/index