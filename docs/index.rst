Ionbeam Documentation
=====================

Event-driven platform for stream-based processing of IoT observations.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   messaging-interface
   ionbeam-client/index
   adr/index

What is IonBeam?
=================

**IonBeam** is an event-driven platform for **stream-based processing of IoT observations**. It allows observational IoT data to be:

1. 🔗 **Ingested** from many sources: REST APIs, MQTT brokers, file servers, etc.
2. 🔄 **Transformed**, cleaned, combined and aggregated
3. 💾 **Output** into multiple storage formats

**Ionbeam** uses an **event-sourcing** architecture. Incoming IoT observations are captured as events, stored, and processed into projections - query-optimized read models tailored to specific downstream use-cases.

Key Capabilities
================

- **Multi-source data ingestion** from various IoT environmental monitoring platforms
- **Real-time data processing** with configurable aggregation windows
- **Standardized data models** following CF (Climate and Forecast) conventions
- **Projection-based event-driven architecture** enabling decoupled, scalable read models tailored to specific query patterns and user needs

Architecture Overview
=====================

The platform consists of three primary component types:

**Data Sources**
  External data collectors that ingest observations from third-party systems and publish them to ionbeam core.

**Ionbeam Core**
  Central processing system that validates, aggregates, and builds time-windowed datasets from raw observations.

**Exporters (Projection Services)**
  Downstream consumers that transform and export datasets to external systems and formats.

Event-Driven Architecture
==========================

Ionbeam follows an event-driven, CQRS-oriented architecture designed to decouple ingestion, aggregation, and read-based concerns.

Incoming data flows as commands that initiate ingestion processes, while events represent immutable facts like "data has been ingested" or "a dataset is now available." These events are broadcast so multiple downstream consumers can react independently without introducing tight coupling.

This separation keeps the ingestion pipeline focused on write concerns - namely collecting, validating, and normalizing raw data — while read concerns, like generating specialized formats, are handled by dedicated projection workers. For example, projections can produce GeoParquet files for a PyGeoAPI-backed EDR API or ODB files for domain-specific workflows without impacting ingestion throughput or latency.

By decoupling these responsibilities, Ionbeam is easier to scale, safer to evolve, and inherently auditable. New data sources or projection formats can be added without disrupting existing processes, and the immutable event stream ensures historical state can always be reconstructed when needed.