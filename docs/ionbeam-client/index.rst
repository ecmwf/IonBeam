Ionbeam Client
==============

Python client library for ingesting observations into Ionbeam and exporting processed datasets.

Installation
------------

**Requirements:** Python 3.12 or later

.. code-block:: bash

   uv pip install ionbeam-client

Configuration
-------------

.. autoclass:: ionbeam_client.config.IonbeamClientConfig
   :members:
   :exclude-members: model_config, model_fields, model_computed_fields, amqp_url, connection_timeout, max_retries, retry_delay

Client API
----------

.. autoclass:: ionbeam_client.client.IonbeamClient
   :members:
   :special-members: __init__, __aenter__, __aexit__

.. autofunction:: ionbeam_client.client.ingest

See Also
--------

- :ref:`messaging-interface:Messaging Interface Specification` - Message contracts
- :ref:`messaging-interface:IngestionMetadata` - Metadata structure details
- :ref:`architecture:Architecture` - System architecture and message flow