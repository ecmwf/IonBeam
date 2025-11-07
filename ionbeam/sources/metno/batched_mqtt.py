import asyncio
import json
import ssl
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator, List, Optional

import aiomqtt
import pandas as pd
import structlog
from pydantic import BaseModel


class MessageBatch(BaseModel):
    messages: List[dict]
    start_time: datetime
    end_time: datetime
    
    class Config:
        arbitrary_types_allowed = True


# Type alias for batch handler callback
BatchHandlerCallback = Callable[[MessageBatch], Awaitable[None]]


@asynccontextmanager
async def subscribe_with_batching(
    hostname: str,
    port: int,
    username: str,
    password: str,
    client_id: str,
    topic: str,
    qos: int,
    batch_handler: BatchHandlerCallback,
    flush_interval_seconds: int = 60,
    flush_max_records: int = 50000,
    max_buffer_size: int = 200000,
    logger_name: str = __name__,
    keepalive: int = 120,
    use_tls: bool = True,
    reconnect_interval: int = 5,
) -> AsyncIterator[None]:
    logger = structlog.get_logger(logger_name)
    buffer: List[dict] = []
    lock = asyncio.Lock()
    stop = asyncio.Event()
    last_flush = time.monotonic()
    
    async def listen():
        """Main MQTT listening loop with automatic reconnection"""
        while not stop.is_set():
            try:
                # Create TLS context if needed
                tls_context: Optional[ssl.SSLContext] = ssl.create_default_context() if use_tls else None
                
                # Create MQTT client with connection parameters
                async with aiomqtt.Client(
                    hostname=hostname,
                    port=port,
                    username=username,
                    password=password,
                    identifier=client_id,
                    keepalive=keepalive,
                    tls_context=tls_context,
                    clean_session=False,
                ) as client:
                    logger.info("Connected to MQTT broker", hostname=hostname, port=port, topic=topic)
                    
                    # Subscribe to the topic
                    await client.subscribe(topic, qos=qos)
                    logger.info("Subscribed to MQTT topic", topic=topic, qos=qos)
                    
                    async for msg in client.messages:
                        if stop.is_set():
                            break
                        # Only process messages matching our topic
                        if msg.topic.matches(topic):
                            await handle_message(msg)
                            
            except aiomqtt.MqttError as e:
                if stop.is_set():
                    break
                logger.warning(
                    "MQTT connection error, reconnecting",
                    error=str(e),
                    retry_in=reconnect_interval,
                    hostname=hostname,
                    port=port,
                )
                await asyncio.sleep(reconnect_interval)
            except Exception as e:
                if stop.is_set():
                    break
                logger.error(
                    "Unexpected error in MQTT listener, reconnecting",
                    error=str(e),
                    retry_in=reconnect_interval,
                )
                await asyncio.sleep(reconnect_interval)

    async def handle_message(msg):
        """Handle incoming MQTT message"""
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            logger.error("unable to parse message", payload=msg.payload)
            return
        
        async with lock:
            if len(buffer) >= max_buffer_size:
                logger.warning(
                    "Buffer full; dropping message",
                    max_size=max_buffer_size,
                )
                return
            buffer.append(data)

    async def process_batches():
        """Process batches periodically based on size/time thresholds"""
        nonlocal last_flush
        
        try:
            while not stop.is_set():
                elapsed = time.monotonic() - last_flush
                flush_due_to_size = len(buffer) >= flush_max_records
                flush_due_to_time = (len(buffer) > 0) and (elapsed >= flush_interval_seconds)

                if not flush_due_to_size and not flush_due_to_time:
                    sleep_for = min(5, max(0, flush_interval_seconds - elapsed))
                    await asyncio.sleep(sleep_for)
                    continue

                async with lock:
                    drained = buffer.copy()
                    buffer.clear()

                if not drained:
                    await asyncio.sleep(1)
                    continue

                # Extract timestamps to determine batch time range
                times = pd.to_datetime(
                    [d.get("properties", {}).get("datetime") for d in drained],
                    utc=True,
                    errors="coerce",
                ).dropna()
                
                if times.empty:
                    logger.warning("No valid datetimes in batch; skipping", messages=len(drained))
                    continue
                    
                start_time = times.min().to_pydatetime()
                end_time = times.max().to_pydatetime()

                # Create batch and invoke handler
                batch = MessageBatch(
                    messages=drained,
                    start_time=start_time,
                    end_time=end_time,
                )

                try:
                    await batch_handler(batch)
                except Exception as e:
                    logger.error(
                        "Batch handler failed",
                        error=str(e),
                        messages=len(drained),
                        start=start_time.isoformat(),
                        end=end_time.isoformat(),
                    )
                    
                last_flush = time.monotonic()
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Unexpected error in batch processing loop", error=str(e))

    # Start background tasks
    mqtt_task = asyncio.create_task(listen())
    batch_task = asyncio.create_task(process_batches())
    
    try:
        yield
    finally:
        # Cleanup
        stop.set()
        
        mqtt_task.cancel()
        batch_task.cancel()
        
        results = await asyncio.gather(mqtt_task, batch_task, return_exceptions=True)
        for idx, res in enumerate(results):
            if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                logger.warning("Task raised during cleanup", task_idx=idx, error=str(res))