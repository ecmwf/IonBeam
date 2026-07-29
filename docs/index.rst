IonBeam
=======

IonBeam is an orchestration system for bringing IoT and other unconventional observations into meteorological workflows. Behind a single Arrow Flight endpoint, the core service:

* Schedules data sources and ingests the observation streams they push
* Validates and normalises observations into a canonical, self-describing schema; variables optionally carry typed semantics from a governed vocabulary, such as CF (Climate and Forecast) standard names
* Builds time-windowed datasets and rebuilds them as late data arrives
* Publishes built datasets as canonical GeoParquet, pushing availability events to subscribed exporters and streaming the data back to them over Flight

Data sources and exporters run as separate Flight clients built on a shared client library, so new ones are added without changing the core. Every component runs with any number of replicas (:ref:`architecture:Scaling`).

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture
   domain
   flight-interface
   dataset-schema
   ionbeam-client/index
