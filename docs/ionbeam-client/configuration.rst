Configuration
=============

The client requires a message broker connection:

.. code-block:: python

   from ionbeam_client import IonbeamClientConfig

   config = IonbeamClientConfig(
       amqp_url="amqp://guest:guest@localhost:5672/"
   )

Key parameters:

**amqp_url** (required)
   RabbitMQ connection string

**connection_timeout** (optional, default: 30)
   Connection timeout in seconds

**max_retries** (optional, default: 3)
   Retry attempts for failed operations

**retry_delay** (optional, default: 1.0)
   Delay between retries in seconds

**write_batch_size** (optional, default: 65536)
   Batch size for export handlers