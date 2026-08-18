IonBeam
=======

IonBeam brings observations from IoT and other unconventional sources into meteorological workflows. It exposes a single Arrow Flight endpoint through which the core service:

* Sends scheduled triggers to data sources and receives their observation streams
* Validates and normalises observations against a declared schema
* Builds time-windowed datasets and revises them when late data arrives
* Notifies exporters when datasets are available and streams the requested data

Variables may use typed semantics from a governed vocabulary, such as CF (Climate and Forecast) standard names. Built geographic datasets use GeoParquet.

Data sources and exporters are separate services built with a shared client library. Valkey coordinates work across replicas where a component supports horizontal scaling (:ref:`architecture:Scaling`).

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture
   domain
   flight-interface
   dataset-schema
   using-the-data
   ionbeam-client/index
