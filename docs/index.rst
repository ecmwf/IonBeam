Ionbeam
=====================

**IonBeam** is an event-driven platform for **stream-based processing of IoT observations**. It enables real-time collection, transformation, and distribution of environmental monitoring data from diverse IoT sources.

The platform operates as a message-driven pipeline that:

1. 🔗 **Ingests** observational data from multiple sources (REST APIs, MQTT brokers, file servers)
2. 🔄 **Transforms** and validates observations using standardized data models following CF (Climate and Forecast) conventions
3. 📊 **Aggregates** data into configurable time windows for efficient querying
4. 💾 **Exports** to multiple formats and systems simultaneously (ODB, GeoParquet, etc..)

Built on event-sourcing principles, Ionbeam captures incoming observations as immutable events. These events are processed into specialized query-optimized read models tailored to specific downstream use-cases. This architecture decouples data ingestion from export concerns, allowing independent scaling and evolution of each component.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture
   messaging-interface
   ionbeam-client/index