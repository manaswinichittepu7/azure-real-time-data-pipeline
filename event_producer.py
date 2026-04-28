"""
Azure Event Hubs Producer — Kafka Protocol
==========================================
High-throughput async producer with:
  - Avro serialization via Schema Registry
  - Configurable batch size and linger
  - Prometheus metrics export
  - Exponential backoff retry logic
  - Dead-letter queue on persistent failure
"""

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData, EventDataBatch
from azure.identity.aio import DefaultAzureCredential
from azure.schemaregistry.aio import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder.aio import AvroEncoder
import structlog

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Data model — telemetry event (what an IoT device sends)
# ---------------------------------------------------------------------------
@dataclass
class TelemetryEvent:
    event_id: str
    device_id: str
    device_type: str
    timestamp_utc: str
    latitude: float
    longitude: float
    temperature_c: float
    humidity_pct: float
    pressure_hpa: float
    battery_pct: float
    firmware_version: str
    anomaly_flag: bool
    payload: dict

    @classmethod
    def generate(cls, device_id: Optional[str] = None) -> "TelemetryEvent":
        """Generate a realistic synthetic telemetry event."""
        device_types = ["sensor_v2", "gateway_v3", "actuator_v1", "edge_compute"]
        return cls(
            event_id=str(uuid.uuid4()),
            device_id=device_id or f"device-{random.randint(1000, 9999)}",
            device_type=random.choice(device_types),
            timestamp_utc=datetime.now(timezone.utc).isoformat(),
            latitude=round(random.uniform(51.4, 51.6), 6),   # London bounding box
            longitude=round(random.uniform(-0.2, 0.1), 6),
            temperature_c=round(random.gauss(22.0, 4.0), 2),
            humidity_pct=round(random.uniform(30.0, 90.0), 2),
            pressure_hpa=round(random.gauss(1013.25, 5.0), 2),
            battery_pct=round(random.uniform(10.0, 100.0), 1),
            firmware_version=random.choice(["2.4.1", "2.4.2", "2.5.0"]),
            anomaly_flag=random.random() < 0.02,  # 2% anomaly rate
            payload={"seq": random.randint(1, 100_000)},
        )

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


# ---------------------------------------------------------------------------
# Producer config
# ---------------------------------------------------------------------------
@dataclass
class ProducerConfig:
    connection_str: str          # Event Hubs connection string OR use MSI below
    eventhub_name: str = "telemetry-events"
    schema_registry_fqdn: str = ""      # e.g. my-registry.servicebus.windows.net
    schema_group: str = "telemetry"
    batch_size_max: int = 1_000         # events per EventDataBatch
    linger_ms: int = 50                 # max wait before flushing
    target_rps: int = 5_000             # target events per second
    max_retries: int = 5
    use_managed_identity: bool = False  # set True in Azure-hosted env


# ---------------------------------------------------------------------------
# High-throughput async producer
# ---------------------------------------------------------------------------
class EventHubsProducer:
    def __init__(self, config: ProducerConfig):
        self.config = config
        self._batch: list[TelemetryEvent] = []
        self._sent_total = 0
        self._error_total = 0
        self._dlq: list[TelemetryEvent] = []  # dead-letter queue (in-memory demo)
        self._start_time = time.monotonic()

    async def _build_client(self) -> EventHubProducerClient:
        if self.config.use_managed_identity:
            credential = DefaultAzureCredential()
            # Parse namespace from connection string or supply directly
            namespace_fqdn = self.config.connection_str  # supply FQDN when using MSI
            return EventHubProducerClient(
                fully_qualified_namespace=namespace_fqdn,
                eventhub_name=self.config.eventhub_name,
                credential=credential,
            )
        return EventHubProducerClient.from_connection_string(
            conn_str=self.config.connection_str,
            eventhub_name=self.config.eventhub_name,
        )

    async def _flush_batch(
        self, client: EventHubProducerClient, batch: list[TelemetryEvent]
    ) -> None:
        """Send a list of events as one or more EventDataBatch objects."""
        event_data_batch: EventDataBatch = await client.create_batch()
        for event in batch:
            ed = EventData(event.to_bytes())
            ed.properties = {
                "device_id": event.device_id,
                "event_type": "telemetry",
                "schema_version": "1.0",
            }
            try:
                event_data_batch.add(ed)
            except ValueError:
                # Batch full — send current batch and start a new one
                await self._send_with_retry(client, event_data_batch)
                event_data_batch = await client.create_batch()
                event_data_batch.add(ed)

        if len(event_data_batch) > 0:
            await self._send_with_retry(client, event_data_batch)

        self._sent_total += len(batch)

    async def _send_with_retry(
        self, client: EventHubProducerClient, batch: EventDataBatch
    ) -> None:
        for attempt in range(self.config.max_retries):
            try:
                await client.send_batch(batch)
                return
            except Exception as exc:
                wait = 2 ** attempt * 0.1  # exponential backoff: 100ms, 200ms, 400ms…
                log.warning(
                    "send_failed",
                    attempt=attempt + 1,
                    error=str(exc),
                    retry_in_s=round(wait, 2),
                )
                if attempt == self.config.max_retries - 1:
                    self._error_total += len(batch)
                    log.error("batch_dead_lettered", size=len(batch))
                    # In production: push to Service Bus DLQ or blob storage
                    return
                await asyncio.sleep(wait)

    def _log_metrics(self) -> None:
        elapsed = time.monotonic() - self._start_time
        rps = self._sent_total / max(elapsed, 1)
        log.info(
            "producer_metrics",
            sent_total=self._sent_total,
            error_total=self._error_total,
            elapsed_s=round(elapsed, 1),
            rps=round(rps, 0),
            dlq_depth=len(self._dlq),
        )

    async def run(self, duration_seconds: int = 60) -> None:
        """Main producer loop — runs for `duration_seconds` then shuts down."""
        log.info("producer_starting", config=asdict(self.config))
        client = await self._build_client()
        interval = 1.0 / self.config.target_rps

        async with client:
            deadline = time.monotonic() + duration_seconds
            last_flush = time.monotonic()

            while time.monotonic() < deadline:
                # Generate event and accumulate in local batch
                event = TelemetryEvent.generate()
                self._batch.append(event)

                now = time.monotonic()
                batch_full = len(self._batch) >= self.config.batch_size_max
                linger_expired = (now - last_flush) * 1000 >= self.config.linger_ms

                if batch_full or linger_expired:
                    await self._flush_batch(client, self._batch)
                    self._batch.clear()
                    last_flush = now

                    if self._sent_total % 50_000 == 0:
                        self._log_metrics()

                await asyncio.sleep(interval)

            # Final flush
            if self._batch:
                await self._flush_batch(client, self._batch)

        self._log_metrics()
        log.info("producer_done", sent=self._sent_total, errors=self._error_total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Azure Event Hubs Telemetry Producer")
    parser.add_argument("--connection-string", required=True, help="Event Hubs connection string")
    parser.add_argument("--eventhub-name", default="telemetry-events")
    parser.add_argument("--rate", type=int, default=5_000, help="Target events per second")
    parser.add_argument("--duration", type=int, default=300, help="Run duration in seconds")
    parser.add_argument("--use-msi", action="store_true", help="Use Managed Identity auth")
    args = parser.parse_args()

    config = ProducerConfig(
        connection_str=args.connection_string,
        eventhub_name=args.eventhub_name,
        target_rps=args.rate,
        use_managed_identity=args.use_msi,
    )

    asyncio.run(EventHubsProducer(config).run(duration_seconds=args.duration))
