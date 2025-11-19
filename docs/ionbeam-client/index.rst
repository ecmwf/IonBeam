Ionbeam Client
==============

Python client for ingesting observations into Ionbeam and exporting processed datasets.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   configuration
   ingestion
   export

Overview
--------

The client provides:

* **Data ingestion** - Publish observations from custom sources
* **Scheduled collection** - Register handlers triggered by Ionbeam Core
* **Dataset export** - Subscribe to processed dataset availability
* **Streaming processing** - Apache Arrow for efficient memory usage

The client abstracts message queuing and data serialization, allowing focus on source-specific collection and export logic.