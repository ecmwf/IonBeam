IonBeam Client
==============

.. warning::

   This library is under active development and may change significantly. Do not rely on it as a stable interface yet.

Python client library for ingesting observations into IonBeam and consuming built datasets. It implements the :doc:`Flight interface <../flight-interface>`: registration, ingestion, trigger subscriptions, and dataset export handlers.

Installation
------------

``ionbeam-client`` is a member of the IonBeam `uv <https://docs.astral.sh/uv/>`__ workspace and is not yet published to PyPI. Inside the repository, ``uv sync`` installs it. To use it from another project, install it from the repository source:

.. code-block:: bash

   uv add ionbeam-client --path <path-to-ionbeam>/ionbeam-client

Python 3.12 or later is required.

Configuration
-------------

.. autoclass:: ionbeam_client.config.IonbeamClientConfig
   :members:
   :exclude-members: model_config, model_fields, model_computed_fields, flight_url, retry_delay, write_batch_size

Client API
----------

.. autoclass:: ionbeam_client.client.IonbeamClient
   :members:
   :special-members: __init__, __aenter__, __aexit__
