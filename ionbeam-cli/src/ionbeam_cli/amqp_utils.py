# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio

import aio_pika
import structlog
from pydantic import BaseModel

from .config import CLIConfig

logger = structlog.get_logger(__name__)


async def publish_message(
    config: CLIConfig,
    message: BaseModel,
    exchange_name: str = "",
    routing_key: str = "",
) -> None:
    message_body = message.model_dump_json()
    
    logger.info(
        "Publishing message",
        exchange=exchange_name or "(default)",
        routing_key=routing_key,
        message_type=type(message).__name__,
    )
    logger.debug("Message payload", body=message_body[:200])
    
    connection = None
    try:
        connection = await aio_pika.connect_robust(
            config.amqp_url,
            timeout=config.connection_timeout,
        )
        
        channel = await connection.channel()
        
        if exchange_name:
            exchange = await channel.declare_exchange(
                exchange_name,
                aio_pika.ExchangeType.FANOUT,
                durable=True,
            )
        else:
            exchange = await channel.get_exchange("")
        
        await exchange.publish(
            aio_pika.Message(
                body=message_body.encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )
        
        logger.info("Message published successfully")
        
    except aio_pika.exceptions.AMQPConnectionError as e:
        logger.error("Failed to connect to RabbitMQ", error=str(e), url=config.amqp_url)
        raise Exception(
            f"Failed to connect to RabbitMQ at {config.amqp_url}. "
            "Check that RabbitMQ is running and credentials are correct."
        ) from e
    except asyncio.TimeoutError as e:
        logger.error("Connection timeout", timeout=config.connection_timeout)
        raise Exception(
            f"Connection to RabbitMQ timed out after {config.connection_timeout}s. "
            "Check network connectivity and RabbitMQ availability."
        ) from e
    except Exception as e:
        logger.error("Failed to publish message", error=str(e), error_type=type(e).__name__)
        raise Exception(f"Failed to publish message: {e}") from e
    finally:
        if connection:
            await connection.close()
            logger.debug("Closed AMQP connection")