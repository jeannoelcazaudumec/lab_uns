#!/usr/bin/env python3
"""
Production-Grade Containerized MQTT-to-AWS Kinesis Ingestion Service

Features:
- MQTT Persistent Session (clean_session=False, QoS 1) on UNS telemetry topics
- Automatic Stream Provisioning with ACTIVE status waiter
- 25 KB Put Payload Unit (PPU) Record Aggregation (NDJSON format)
- Gzip level 6 compression for 65%-85%+ payload size reduction
- PutRecords multi-record batching (count, byte size, age triggers)
- High-cardinality partition keys for uniform shard distribution
- Persistent boto3 client with connection pooling (25) & TCP Keep-Alive
- Resilient jittered exponential backoff retrying only failed records
- Live real-time PPU cost metrics and byte savings reporting on every flush
- Graceful shutdown handling SIGINT/SIGTERM with full memory buffer drain
"""

import gzip
import json
import logging
import math
import os
import random
import signal
import sys
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Timezone & Logging Setup
# ---------------------------------------------------------------------------
if hasattr(time, "tzset"):
    try:
        time.tzset()
    except Exception as e:
        sys.stderr.write(f"Warning: time.tzset() failed: {e}\n")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("mqtt_to_kinesis")

# ---------------------------------------------------------------------------
# Configuration Parameters
# ---------------------------------------------------------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-uns")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv(
    "MQTT_TOPIC", "enterprise/plant/area/sub-area/cell/+/telemetry/"
)
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-to-kinesis-ingestor")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

KINESIS_ENDPOINT_URL = os.getenv("KINESIS_ENDPOINT_URL", "http://localstack:4566")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "iiot-telemetry-stream")
KINESIS_SHARD_COUNT = int(os.getenv("KINESIS_SHARD_COUNT", "1"))
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Tier 1: Record Aggregation into single NDJSON record (<25 KB PPU boundary)
# 25 KB = 25,600 bytes. Default ~24 KB (24,576 bytes) safely fits within 1 PPU.
PPU_UNIT_BYTES = 25600
AGGREGATION_MAX_RECORDS = int(os.getenv("AGGREGATION_MAX_RECORDS", "50"))
AGGREGATION_MAX_BYTES = int(os.getenv("AGGREGATION_MAX_BYTES", "24576"))
AGGREGATION_MAX_SECONDS = float(os.getenv("AGGREGATION_MAX_SECONDS", "5.0"))
GZIP_COMPRESS_LEVEL = int(os.getenv("GZIP_COMPRESS_LEVEL", "6"))

# Tier 2: PutRecords Batch Buffer (accumulates prepared Kinesis records)
# Kinesis PutRecords API limit: 500 records or 5 MB per call.
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "100"))
BATCH_MAX_BYTES = int(os.getenv("BATCH_MAX_BYTES", str(4 * 1024 * 1024)))  # 4 MB
BATCH_MAX_SECONDS = float(os.getenv("BATCH_MAX_SECONDS", "2.0"))

# Boto3 & Resiliency Settings
BOTO_MAX_CONNECTIONS = int(os.getenv("BOTO_MAX_CONNECTIONS", "25"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_RETRY_DELAY = float(os.getenv("BASE_RETRY_DELAY", "0.2"))
MAX_RETRY_DELAY = float(os.getenv("MAX_RETRY_DELAY", "3.0"))


# ---------------------------------------------------------------------------
# Kinesis Client Factory & Stream Provisioning
# ---------------------------------------------------------------------------
def create_kinesis_client():
    """Create persistent boto3 Kinesis client with connection pooling and TCP keep-alive."""
    boto_config = Config(
        max_pool_connections=BOTO_MAX_CONNECTIONS,
        tcp_keepalive=True,
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=5,
        read_timeout=10,
    )
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )
    return session.client(
        "kinesis",
        endpoint_url=KINESIS_ENDPOINT_URL,
        config=boto_config,
    )


def ensure_stream_exists(kinesis_client, stream_name: str, shard_count: int = 1):
    """
    Verify Kinesis stream existence and wait until ACTIVE status.
    Auto-creates the stream if not present.
    """
    logger.info(f"Checking status of Kinesis stream '{stream_name}' at {KINESIS_ENDPOINT_URL}...")
    stream_found = False

    try:
        resp = kinesis_client.describe_stream_summary(StreamName=stream_name)
        status = resp["StreamDescriptionSummary"]["StreamStatus"]
        logger.info(f"Stream '{stream_name}' exists with status: '{status}'")
        stream_found = True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "ResourceNotFoundException":
            logger.info(f"Stream '{stream_name}' not found. Initiating auto-creation (Shards={shard_count})...")
            try:
                kinesis_client.create_stream(
                    StreamName=stream_name,
                    ShardCount=shard_count,
                )
                logger.info(f"CreateStream API succeeded for '{stream_name}'.")
            except ClientError as ce:
                # Handle potential race condition if created concurrently
                if ce.response.get("Error", {}).get("Code") != "ResourceInUseException":
                    raise
        else:
            raise

    # Wait for stream to reach ACTIVE status
    logger.info(f"Waiting for stream '{stream_name}' to become ACTIVE...")
    max_wait_seconds = 60
    start_time = time.time()

    while time.time() - start_time < max_wait_seconds:
        try:
            try:
                desc = kinesis_client.describe_stream_summary(StreamName=stream_name)
                status = desc["StreamDescriptionSummary"]["StreamStatus"]
                open_shards = desc["StreamDescriptionSummary"].get("OpenShardCount", shard_count)
            except Exception:
                desc = kinesis_client.describe_stream(StreamName=stream_name)
                status = desc["StreamDescription"]["StreamStatus"]
                open_shards = len(desc["StreamDescription"].get("Shards", []))

            if status == "ACTIVE":
                logger.info(f"Stream '{stream_name}' is ACTIVE and ready ({open_shards} open shard(s)).")
                return
            logger.info(f"Stream status is '{status}', waiting 1s...")
        except ClientError as e:
            logger.warning(f"Waiting for stream readiness: {e}")
        time.sleep(1.0)

    raise TimeoutError(f"Stream '{stream_name}' did not transition to ACTIVE within {max_wait_seconds}s.")


# ---------------------------------------------------------------------------
# High-Cardinality Partition Key Generator
# ---------------------------------------------------------------------------
def extract_asset_id(topic: str, payload_dict: Optional[Dict[str, Any]]) -> str:
    """Extract asset_id from payload or MQTT topic."""
    if payload_dict and isinstance(payload_dict, dict):
        asset = payload_dict.get("asset_id")
        if asset:
            return str(asset)
    parts = [p for p in topic.split("/") if p]
    for idx, part in enumerate(parts):
        if part.startswith("asset_"):
            return part
        if part == "telemetry" and idx > 0:
            return parts[idx - 1]
    return "asset_unknown"


def generate_partition_key(asset_id: str) -> str:
    """
    Generate high-cardinality partition key to ensure uniform MD5 hashing
    across Kinesis shard hash-key ranges, preventing hot shards.
    """
    return f"{asset_id}_{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Two-Tier Ingestion Service Engine
# ---------------------------------------------------------------------------
class KinesisIngestionService:
    def __init__(self, kinesis_client):
        self.kinesis_client = kinesis_client
        self.stream_name = KINESIS_STREAM_NAME
        self.lock = threading.Lock()
        self.running = True

        # Tier 1: Record Aggregation Buffer (raw MQTT events -> 1 Kinesis Record)
        self.agg_events: List[str] = []
        self.agg_event_sizes: List[int] = []
        self.agg_raw_bytes: int = 0
        self.agg_start_time: float = 0.0
        self.agg_asset_ids: List[str] = []

        # Tier 2: PutRecords Batch Buffer (prepared Kinesis records -> PutRecords API)
        self.batch_records: List[Dict[str, Any]] = []
        self.batch_meta: List[Dict[str, Any]] = []
        self.batch_bytes: int = 0
        self.batch_start_time: float = 0.0

        # Flushing & Thread Coordination
        self.flush_trigger = threading.Event()
        self.worker_thread = threading.Thread(target=self._flush_loop, name="kinesis-flusher", daemon=True)

        # Cumulative Metrics Tracking
        self.cum_flushes: int = 0
        self.cum_events: int = 0
        self.cum_kinesis_records: int = 0
        self.cum_uncompressed_bytes: int = 0
        self.cum_compressed_bytes: int = 0
        self.cum_unopt_ppus: int = 0
        self.cum_actual_ppus: int = 0

    def start(self):
        """Start background worker thread."""
        self.worker_thread.start()
        logger.info("Background Kinesis flush worker thread started.")

    def ingest_event(self, topic: str, payload_str: str):
        """
        Handle incoming MQTT event from MQTT client thread.
        Thread-safe aggregation into NDJSON record.
        """
        with self.lock:
            if not self.running:
                return

            event_bytes_len = len(payload_str.encode("utf-8"))
            parsed_dict = None
            try:
                parsed_dict = json.loads(payload_str)
            except Exception:
                pass

            asset_id = extract_asset_id(topic, parsed_dict)

            # Check if adding this event exceeds aggregation limits
            # Include 1 byte for newline separator
            additional_bytes = event_bytes_len + 1
            exceeds_bytes = (self.agg_raw_bytes + additional_bytes > AGGREGATION_MAX_BYTES) and len(self.agg_events) > 0
            exceeds_count = len(self.agg_events) >= AGGREGATION_MAX_RECORDS

            if exceeds_bytes or exceeds_count:
                self._aggregate_and_queue_under_lock()

            # Append current event to aggregation buffer
            if not self.agg_events:
                self.agg_start_time = time.time()

            self.agg_events.append(payload_str)
            self.agg_event_sizes.append(event_bytes_len)
            self.agg_raw_bytes += additional_bytes
            self.agg_asset_ids.append(asset_id)

            # If a single message was larger than aggregation limit, pack it immediately
            if self.agg_raw_bytes >= AGGREGATION_MAX_BYTES or len(self.agg_events) >= AGGREGATION_MAX_RECORDS:
                self._aggregate_and_queue_under_lock()

            # Check if batch buffer reached trigger
            if len(self.batch_records) >= BATCH_MAX_RECORDS or self.batch_bytes >= BATCH_MAX_BYTES:
                self.flush_trigger.set()

    def _aggregate_and_queue_under_lock(self):
        """
        Aggregate pending MQTT events into a single Gzip-compressed NDJSON Kinesis record.
        MUST BE CALLED WHILE HOLDING self.lock.
        """
        if not self.agg_events:
            return

        events_to_pack = self.agg_events
        sizes_to_pack = self.agg_event_sizes
        assets_to_pack = self.agg_asset_ids

        # Reset aggregation buffer
        self.agg_events = []
        self.agg_event_sizes = []
        self.agg_raw_bytes = 0
        self.agg_start_time = 0.0
        self.agg_asset_ids = []

        # Build NDJSON byte sequence
        ndjson_str = "\n".join(events_to_pack) + "\n"
        ndjson_bytes = ndjson_str.encode("utf-8")
        uncompressed_size = len(ndjson_bytes)

        # Gzip compression (level 6)
        compressed_bytes = gzip.compress(ndjson_bytes, compresslevel=GZIP_COMPRESS_LEVEL)
        compressed_size = len(compressed_bytes)

        # Determine partition key
        primary_asset = assets_to_pack[0] if assets_to_pack else "asset_unknown"
        partition_key = generate_partition_key(primary_asset)

        # PPU Calculations
        # Unoptimized: Each raw MQTT message sent as individual record -> ceil(len / 25600)
        unopt_ppus = sum(math.ceil(sz / PPU_UNIT_BYTES) for sz in sizes_to_pack)
        # Actual: Single aggregated compressed record -> ceil(compressed / 25600)
        actual_ppus = math.ceil(compressed_size / PPU_UNIT_BYTES)

        kinesis_record = {
            "Data": compressed_bytes,
            "PartitionKey": partition_key,
        }
        meta_info = {
            "event_count": len(events_to_pack),
            "uncompressed_bytes": uncompressed_size,
            "compressed_bytes": compressed_size,
            "unopt_ppus": unopt_ppus,
            "actual_ppus": actual_ppus,
            "partition_key": partition_key,
        }

        if not self.batch_records:
            self.batch_start_time = time.time()

        self.batch_records.append(kinesis_record)
        self.batch_meta.append(meta_info)
        self.batch_bytes += compressed_size

        logger.debug(
            f"Aggregated {len(events_to_pack)} events into 1 record: "
            f"{uncompressed_size} B raw -> {compressed_size} B gzip ({actual_ppus} PPU vs {unopt_ppus} unopt PPUs)"
        )

    def _flush_loop(self):
        """Background loop monitoring aggregation and batch timeouts."""
        while self.running:
            # Wake up on trigger or periodic check interval (every 200ms)
            self.flush_trigger.wait(timeout=0.2)
            self.flush_trigger.clear()

            now = time.time()
            records_to_send: List[Dict[str, Any]] = []
            meta_to_report: List[Dict[str, Any]] = []

            with self.lock:
                # 1. Check if aggregation buffer is overdue
                if self.agg_events and (now - self.agg_start_time >= AGGREGATION_MAX_SECONDS):
                    self._aggregate_and_queue_under_lock()

                # 2. Check if batch buffer is ready to flush (count, bytes, or overdue timeout)
                batch_ready = False
                if self.batch_records:
                    if len(self.batch_records) >= BATCH_MAX_RECORDS:
                        batch_ready = True
                    elif self.batch_bytes >= BATCH_MAX_BYTES:
                        batch_ready = True
                    elif now - self.batch_start_time >= BATCH_MAX_SECONDS:
                        batch_ready = True

                if batch_ready and self.batch_records:
                    records_to_send = self.batch_records
                    meta_to_report = self.batch_meta
                    self.batch_records = []
                    self.batch_meta = []
                    self.batch_bytes = 0
                    self.batch_start_time = 0.0

            # Execute Kinesis PutRecords OUTSIDE the lock to avoid blocking ingestion
            if records_to_send:
                self._send_batch_with_retry(records_to_send, meta_to_report)

    def _send_batch_with_retry(
        self,
        records: List[Dict[str, Any]],
        meta_list: List[Dict[str, Any]],
    ):
        """
        Send batch to AWS Kinesis via PutRecords API with resilient jittered retry
        on partial failures (e.g. ProvisionedThroughputExceededException).
        Never duplicates successfully written records.
        """
        records_to_send = list(records)
        attempt = 0
        total_records = len(records)
        successful_records = 0

        while records_to_send and attempt <= MAX_RETRIES:
            attempt += 1
            try:
                response = self.kinesis_client.put_records(
                    StreamName=self.stream_name,
                    Records=records_to_send,
                )

                failed_count = response.get("FailedRecordCount", 0)
                records_result = response.get("Records", [])

                if failed_count == 0:
                    successful_records += len(records_to_send)
                    records_to_send = []
                    break

                # Extract only records that encountered errors for retry
                failed_records = []
                for idx, res in enumerate(records_result):
                    if "ErrorCode" in res:
                        err_code = res.get("ErrorCode")
                        err_msg = res.get("ErrorMessage", "")
                        logger.warning(
                            f"[PutRecords Partial Failure] Record {idx} failed with {err_code}: {err_msg}"
                        )
                        failed_records.append(records_to_send[idx])
                    else:
                        successful_records += 1

                records_to_send = failed_records
                if records_to_send and attempt <= MAX_RETRIES:
                    jitter = random.uniform(0.05, 0.25)
                    backoff = min(MAX_RETRY_DELAY, BASE_RETRY_DELAY * (2 ** (attempt - 1))) + jitter
                    logger.info(
                        f"Retrying {len(records_to_send)} failed records in {backoff:.3f}s (attempt {attempt}/{MAX_RETRIES})..."
                    )
                    time.sleep(backoff)

            except (ClientError, EndpointConnectionError, Exception) as e:
                logger.error(f"[PutRecords Exception] Attempt {attempt}/{MAX_RETRIES} failed: {e}")
                if attempt <= MAX_RETRIES:
                    jitter = random.uniform(0.1, 0.5)
                    backoff = min(MAX_RETRY_DELAY, BASE_RETRY_DELAY * (2 ** (attempt - 1))) + jitter
                    time.sleep(backoff)
                else:
                    logger.critical(f"Dropping {len(records_to_send)} records after {MAX_RETRIES} retries exhausted.")
                    records_to_send = []

        # Compute and log Live PPU Cost Metrics
        self._record_and_log_metrics(total_records, successful_records, meta_list)

    def _record_and_log_metrics(
        self,
        total_records: int,
        successful_records: int,
        meta_list: List[Dict[str, Any]],
    ):
        """Log real-time PPU cost savings and payload compression metrics."""
        batch_events = sum(m["event_count"] for m in meta_list)
        batch_uncompressed_bytes = sum(m["uncompressed_bytes"] for m in meta_list)
        batch_compressed_bytes = sum(m["compressed_bytes"] for m in meta_list)
        batch_unopt_ppus = sum(m["unopt_ppus"] for m in meta_list)
        batch_actual_ppus = sum(m["actual_ppus"] for m in meta_list)

        # Percentage savings
        byte_savings_pct = (
            ((batch_uncompressed_bytes - batch_compressed_bytes) / batch_uncompressed_bytes * 100.0)
            if batch_uncompressed_bytes > 0
            else 0.0
        )
        ppu_savings_pct = (
            ((batch_unopt_ppus - batch_actual_ppus) / batch_unopt_ppus * 100.0)
            if batch_unopt_ppus > 0
            else 0.0
        )

        with self.lock:
            self.cum_flushes += 1
            self.cum_events += batch_events
            self.cum_kinesis_records += successful_records
            self.cum_uncompressed_bytes += batch_uncompressed_bytes
            self.cum_compressed_bytes += batch_compressed_bytes
            self.cum_unopt_ppus += batch_unopt_ppus
            self.cum_actual_ppus += batch_actual_ppus

            cum_byte_savings = (
                ((self.cum_uncompressed_bytes - self.cum_compressed_bytes) / self.cum_uncompressed_bytes * 100.0)
                if self.cum_uncompressed_bytes > 0
                else 0.0
            )
            cum_ppu_savings = (
                ((self.cum_unopt_ppus - self.cum_actual_ppus) / self.cum_unopt_ppus * 100.0)
                if self.cum_unopt_ppus > 0
                else 0.0
            )

        logger.info(
            f"[PPU METRICS] Flush #{self.cum_flushes}: {batch_events} events -> {successful_records} Kinesis record(s) | "
            f"Raw: {batch_uncompressed_bytes:,} B -> Gzip: {batch_compressed_bytes:,} B ({byte_savings_pct:.1f}% byte savings) | "
            f"PPUs: {batch_unopt_ppus} unoptimized -> {batch_actual_ppus} billed ({ppu_savings_pct:.1f}% cost reduction) | "
            f"Cumulative: {self.cum_events:,} events, {self.cum_unopt_ppus:,} unopt PPUs vs {self.cum_actual_ppus:,} billed PPUs "
            f"({cum_ppu_savings:.1f}% total cost savings, {cum_byte_savings:.1f}% byte savings)"
        )

    def flush_and_drain(self):
        """Force flush all buffered events and records during graceful shutdown."""
        logger.info("Draining in-memory aggregation and batch buffers...")
        with self.lock:
            # Drain Tier 1 aggregation into Tier 2 batch
            if self.agg_events:
                self._aggregate_and_queue_under_lock()

            records_to_drain = self.batch_records
            meta_to_drain = self.batch_meta
            self.batch_records = []
            self.batch_meta = []
            self.batch_bytes = 0

        # Send final batch
        if records_to_drain:
            logger.info(f"Flushing final {len(records_to_drain)} record(s) to Kinesis...")
            self._send_batch_with_retry(records_to_drain, meta_to_drain)
        else:
            logger.info("Buffers are empty. No final flush required.")

        # Print final metrics
        logger.info("=" * 75)
        logger.info("FINAL INGESTION & COST SAVINGS SUMMARY")
        logger.info(f"Total Flushes:              {self.cum_flushes}")
        logger.info(f"Total Telemetry Events:     {self.cum_events:,}")
        logger.info(f"Kinesis Records Written:    {self.cum_kinesis_records:,}")
        logger.info(f"Uncompressed Data:          {self.cum_uncompressed_bytes:,} bytes")
        logger.info(f"Compressed Data Written:    {self.cum_compressed_bytes:,} bytes")
        cum_bytes_saved = self.cum_uncompressed_bytes - self.cum_compressed_bytes
        cum_byte_pct = (cum_bytes_saved / self.cum_uncompressed_bytes * 100.0) if self.cum_uncompressed_bytes > 0 else 0.0
        logger.info(f"Total Bytes Saved:          {cum_bytes_saved:,} bytes ({cum_byte_pct:.1f}%)")
        logger.info(f"Unoptimized PPUs (1/msg):   {self.cum_unopt_ppus:,} PPUs")
        logger.info(f"Actual Billed PPUs (25KB):  {self.cum_actual_ppus:,} PPUs")
        ppus_saved = self.cum_unopt_ppus - self.cum_actual_ppus
        ppu_pct = (ppus_saved / self.cum_unopt_ppus * 100.0) if self.cum_unopt_ppus > 0 else 0.0
        logger.info(f"Total PPUs Saved:           {ppus_saved:,} PPUs ({ppu_pct:.1f}% billable cost savings)")
        logger.info("=" * 75)


# ---------------------------------------------------------------------------
# MQTT Callback Handlers
# ---------------------------------------------------------------------------
ingestion_service: Optional[KinesisIngestionService] = None
shutdown_requested = False


def on_mqtt_connect(client, userdata, flags, rc):
    """Callback when connected to the MQTT Broker."""
    if rc == 0:
        logger.info(f"Connected to Mosquitto UNS broker at {MQTT_HOST}:{MQTT_PORT} (rc=0)")
        # Subscribe with QoS 1 and persistent session (clean_session=False)
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info(f"Subscribed with QoS 1 to validated telemetry topic: '{MQTT_TOPIC}'")
    else:
        logger.error(f"Failed to connect to Mosquitto UNS broker, return code: {rc}")


def on_mqtt_disconnect(client, userdata, rc):
    """Callback when disconnected from the MQTT Broker."""
    if rc != 0:
        logger.warning(f"Unexpected MQTT disconnection (rc={rc}). Will automatically reconnect...")
    else:
        logger.info("Clean MQTT disconnection completed.")


def on_mqtt_message(client, userdata, msg):
    """Callback when a telemetry message arrives."""
    global ingestion_service
    if shutdown_requested or not ingestion_service:
        return

    try:
        payload_str = msg.payload.decode("utf-8", errors="replace")
        ingestion_service.ingest_event(msg.topic, payload_str)
    except Exception as e:
        logger.error(f"Error handling message on {msg.topic}: {e}")


def handle_shutdown(signum, frame):
    """Handle SIGINT and SIGTERM for graceful shutdown."""
    global shutdown_requested
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    logger.info(f"Received {sig_name}. Initiating graceful shutdown...")
    shutdown_requested = True


# ---------------------------------------------------------------------------
# Main Application Entry Point
# ---------------------------------------------------------------------------
def main():
    global ingestion_service, shutdown_requested

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("=" * 75)
    logger.info("Starting Containerized MQTT-to-AWS Kinesis Ingestion Service")
    logger.info(f"Kinesis Endpoint:        {KINESIS_ENDPOINT_URL}")
    logger.info(f"Kinesis Stream:          {KINESIS_STREAM_NAME} (Shards: {KINESIS_SHARD_COUNT})")
    logger.info(f"MQTT Broker:             {MQTT_HOST}:{MQTT_PORT}")
    logger.info(f"MQTT Topic:              {MQTT_TOPIC} (QoS 1, clean_session=False)")
    logger.info(f"Aggregation Limits:      {AGGREGATION_MAX_RECORDS} records | {AGGREGATION_MAX_BYTES} bytes (~24 KB) | {AGGREGATION_MAX_SECONDS}s")
    logger.info(f"Gzip Compression:        Level {GZIP_COMPRESS_LEVEL}")
    logger.info(f"PutRecords Batch Limits: {BATCH_MAX_RECORDS} records | {BATCH_MAX_BYTES} bytes (4 MB) | {BATCH_MAX_SECONDS}s")
    logger.info("=" * 75)

    # 1. Initialize Kinesis Client and Verify/Provision Stream
    kinesis_client = create_kinesis_client()
    try:
        ensure_stream_exists(kinesis_client, KINESIS_STREAM_NAME, KINESIS_SHARD_COUNT)
    except Exception as e:
        logger.critical(f"Failed to verify or provision Kinesis stream: {e}")
        sys.exit(1)

    # 2. Initialize Ingestion Engine
    ingestion_service = KinesisIngestionService(kinesis_client)
    ingestion_service.start()

    # 3. Setup Persistent MQTT Client (clean_session=False, QoS 1)
    mqtt_client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        clean_session=False,
    )
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_disconnect = on_mqtt_disconnect
    mqtt_client.on_message = on_mqtt_message

    # Reconnect resilience
    connected = False
    retry_delay = 2.0
    while not connected and not shutdown_requested:
        try:
            logger.info(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
            connected = True
        except Exception as e:
            logger.warning(f"MQTT connection failed: {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
            retry_delay = min(15.0, retry_delay * 1.5)

    if shutdown_requested:
        sys.exit(0)

    mqtt_client.loop_start()

    # Main thread lifecycle loop
    try:
        while not shutdown_requested:
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")

    # Graceful Shutdown Sequence
    logger.info("Stopping MQTT client network loop...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

    if ingestion_service:
        ingestion_service.running = False
        ingestion_service.flush_trigger.set()
        # Drain all buffers
        ingestion_service.flush_and_drain()

    logger.info("MQTT-to-Kinesis ingestion service exited cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()
