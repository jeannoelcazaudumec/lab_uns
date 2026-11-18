#!/usr/bin/env python3
"""
Production-Grade MQTT-to-S3 Data Lake Ingestion Microservice

Features:
- Dynamic in-memory multi-trigger buffer (count, byte size, age window)
- Newline-Delimited JSON (NDJSON) serialization + Gzip level 6 compression
- Hive-style partitioned S3 object keys (year=YYYY/month=MM/day=DD/hour=HH)
- S3 metadata enrichment (record count, byte sizes, ingestion timestamp)
- Persistent boto3 client with connection pooling & HTTP Keep-Alive
- Exponential backoff retries on S3 upload failures
- At-least-once delivery semantics (buffer evicted only on HTTP 200 OK)
- Graceful shutdown with buffer flush on SIGTERM / SIGINT
- Full timezone awareness matching local host timezone
"""

import gzip
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
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
logger = logging.getLogger("mqtt_to_s3")

# ---------------------------------------------------------------------------
# Configuration Parameters
# ---------------------------------------------------------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-uns")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv(
    "MQTT_TOPIC", "enterprise/plant/area/sub-area/cell/+/telemetry/"
)
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-to-s3-lake")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "telemetry-lake")
S3_PREFIX = os.getenv("S3_PREFIX", "telemetry")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

BUFFER_MAX_RECORDS = int(os.getenv("BUFFER_MAX_RECORDS", "50"))
BUFFER_MAX_BYTES = int(os.getenv("BUFFER_MAX_BYTES", str(1 * 1024 * 1024)))  # 1 MB default
BUFFER_MAX_SECONDS = float(os.getenv("BUFFER_MAX_SECONDS", "15.0"))  # 15s default
GZIP_COMPRESS_LEVEL = int(os.getenv("GZIP_COMPRESS_LEVEL", "6"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_INITIAL_DELAY = float(os.getenv("RETRY_INITIAL_DELAY", "1.0"))

running = True


# ---------------------------------------------------------------------------
# S3 Client Factory
# ---------------------------------------------------------------------------
def create_s3_client():
    boto_config = Config(
        max_pool_connections=25,
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=5,
        read_timeout=10,
    )
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    return session.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        config=boto_config,
    )


def ensure_bucket_exists(s3_client, bucket_name: str, region: str):
    while running:
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Target S3 bucket '{bucket_name}' verified and ready.")
            return True
        except ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", ""))
            if error_code in ("404", "NoSuchBucket"):
                logger.info(
                    f"Target S3 bucket '{bucket_name}' not found. Auto-creating bucket..."
                )
                try:
                    if region == "us-east-1":
                        s3_client.create_bucket(Bucket=bucket_name)
                    else:
                        s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={"LocationConstraint": region},
                        )
                    logger.info(
                        f"Target S3 bucket '{bucket_name}' created successfully."
                    )
                    return True
                except ClientError as ce:
                    ce_code = ce.response.get("Error", {}).get("Code", "")
                    if ce_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                        logger.info(f"Target S3 bucket '{bucket_name}' already exists.")
                        return True
                    logger.warning(f"Could not create bucket '{bucket_name}': {ce}")
            else:
                logger.warning(
                    f"head_bucket check returned '{error_code}': {e}. Retrying in 2s..."
                )
        except Exception as ex:
            logger.warning(f"S3 endpoint unreachable ({ex}). Retrying in 2s...")

        time.sleep(2.0)
    return False


# ---------------------------------------------------------------------------
# Dynamic In-Memory Buffer
# ---------------------------------------------------------------------------
class IngestionBuffer:
    def __init__(self, s3_client, bucket: str, prefix: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.lock = threading.Lock()
        self.flush_lock = threading.Lock()
        self.records = []  # List of NDJSON strings (1 per record)
        self.current_bytes = 0
        self.last_flush_time = time.time()

        # Cumulative Metrics
        self.total_ingested = 0
        self.total_uploaded = 0
        self.total_uncompressed_bytes = 0
        self.total_compressed_bytes = 0

    def add_record(self, record_str: str):
        record_bytes = len(record_str.encode("utf-8")) + 1  # includes '\n'
        should_flush = False
        flush_reason = None

        with self.lock:
            self.records.append(record_str)
            self.current_bytes += record_bytes
            self.total_ingested += 1

            if len(self.records) >= BUFFER_MAX_RECORDS:
                should_flush = True
                flush_reason = (
                    f"record_count_threshold ({len(self.records)} >= {BUFFER_MAX_RECORDS})"
                )
            elif self.current_bytes >= BUFFER_MAX_BYTES:
                should_flush = True
                flush_reason = (
                    f"byte_size_threshold ({self.current_bytes} >= {BUFFER_MAX_BYTES} B)"
                )

        if should_flush:
            self.flush(reason=flush_reason)

    def check_timer_flush(self):
        should_flush = False
        flush_reason = None
        with self.lock:
            age = time.time() - self.last_flush_time
            if self.records and age >= BUFFER_MAX_SECONDS:
                should_flush = True
                flush_reason = (
                    f"time_window_threshold ({age:.1f}s >= {BUFFER_MAX_SECONDS}s, records={len(self.records)})"
                )

        if should_flush:
            self.flush(reason=flush_reason)

    def flush(self, reason: str = "manual") -> bool:
        with self.flush_lock:
            with self.lock:
                if not self.records:
                    return False
                batch = list(self.records)
                batch_count = len(batch)

            # NDJSON serialization
            ndjson_text = "\n".join(batch) + "\n"
            uncompressed_bytes = len(ndjson_text.encode("utf-8"))

            # Gzip level 6 compression
            compressed_data = gzip.compress(
                ndjson_text.encode("utf-8"), compresslevel=GZIP_COMPRESS_LEVEL
            )
            compressed_bytes = len(compressed_data)
            compression_ratio = (
                (1.0 - (compressed_bytes / uncompressed_bytes)) * 100.0
                if uncompressed_bytes > 0
                else 0.0
            )

            # Hive-style partitioning key
            now_utc = datetime.now(timezone.utc)
            year = now_utc.strftime("%Y")
            month = now_utc.strftime("%m")
            day = now_utc.strftime("%d")
            hour = now_utc.strftime("%H")
            ts_compact = now_utc.strftime("%Y%m%d_%H%M%S")
            file_uuid = uuid.uuid4().hex[:8]
            s3_key = (
                f"{self.prefix}/year={year}/month={month}/day={day}/hour={hour}/"
                f"{self.prefix}_{ts_compact}_{file_uuid}.json.gz"
            )

            metadata = {
                "record-count": str(batch_count),
                "uncompressed-bytes": str(uncompressed_bytes),
                "compressed-bytes": str(compressed_bytes),
                "ingested-at": now_utc.isoformat(),
            }

            # Upload to S3 with exponential backoff retries
            success, upload_latency_ms = self._upload_with_retry(
                s3_key=s3_key,
                data=compressed_data,
                metadata=metadata,
                batch_count=batch_count,
            )

            if success:
                # AT-LEAST-ONCE DELIVERY: Evict only after receiving HTTP 200 OK
                with self.lock:
                    del self.records[:batch_count]
                    self.current_bytes = sum(
                        len(r.encode("utf-8")) + 1 for r in self.records
                    )
                    self.last_flush_time = time.time()
                    self.total_uploaded += batch_count
                    self.total_uncompressed_bytes += uncompressed_bytes
                    self.total_compressed_bytes += compressed_bytes

                logger.info(
                    f"[S3 INGESTION FLUSH] reason='{reason}' batch_size={batch_count} "
                    f"uncompressed={uncompressed_bytes}B compressed={compressed_bytes}B "
                    f"compression_ratio={compression_ratio:.1f}% upload_latency={upload_latency_ms:.2f}ms "
                    f"key=s3://{self.bucket}/{s3_key} buffer_remaining={len(self.records)}"
                )
                return True
            else:
                logger.error(
                    f"[S3 INGESTION FAILED] Could not upload batch of {batch_count} records. "
                    f"Buffer retained (at-least-once guarantee). Current buffer size: {len(self.records)}"
                )
                return False

    def _upload_with_retry(
        self, s3_key: str, data: bytes, metadata: dict, batch_count: int
    ):
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            t0 = time.time()
            try:
                resp = self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=data,
                    ContentEncoding="gzip",
                    ContentType="application/x-ndjson",
                    Metadata=metadata,
                )
                status_code = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status_code == 200:
                    latency_ms = (time.time() - t0) * 1000.0
                    return True, latency_ms
                else:
                    logger.warning(
                        f"S3 put_object returned HTTP status {status_code} "
                        f"(attempt {attempt}/{MAX_RETRIES})"
                    )
            except Exception as e:
                logger.warning(
                    f"S3 put_object attempt {attempt}/{MAX_RETRIES} failed: {e}"
                )

            if attempt < MAX_RETRIES:
                backoff_delay = RETRY_INITIAL_DELAY * (2 ** (attempt - 1))
                logger.info(f"Retrying S3 upload in {backoff_delay:.2f}s...")
                time.sleep(backoff_delay)

        return False, 0.0


# ---------------------------------------------------------------------------
# Background Timer Thread
# ---------------------------------------------------------------------------
def timer_thread_fn(buffer: IngestionBuffer, stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            buffer.check_timer_flush()
        except Exception as e:
            logger.error(f"Error in timer thread check: {e}")
        stop_event.wait(timeout=1.0)


# ---------------------------------------------------------------------------
# MQTT Callbacks
# ---------------------------------------------------------------------------
def create_mqtt_callbacks(buffer: IngestionBuffer):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            session_present = flags.get("session present", 0)
            logger.info(
                f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT} as '{MQTT_CLIENT_ID}' "
                f"(clean_session=False, session_present={session_present})"
            )
            # Subscribe with QoS 1
            res, mid = client.subscribe(MQTT_TOPIC, qos=1)
            logger.info(
                f"Subscribed to topic '{MQTT_TOPIC}' with QoS 1 (mid={mid})"
            )
        else:
            logger.error(f"MQTT connection failed with code {rc}")

    def on_disconnect(client, userdata, rc):
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (rc={rc}). Will auto-reconnect...")
        else:
            logger.info("MQTT client disconnected cleanly.")

    def on_message(client, userdata, msg):
        try:
            payload_str = msg.payload.decode("utf-8")
            parsed = json.loads(payload_str)
            # Serialize to compact 1-line NDJSON format
            compact_json = json.dumps(parsed, separators=(",", ":"))
            buffer.add_record(compact_json)
        except Exception as e:
            logger.warning(
                f"Failed to decode or parse message from topic {msg.topic}: {e}"
            )

    return on_connect, on_disconnect, on_message


# ---------------------------------------------------------------------------
# Main Application Entrypoint
# ---------------------------------------------------------------------------
def main():
    global running

    logger.info("=" * 68)
    logger.info("Starting Production MQTT-to-S3 Data Lake Ingestion Service")
    logger.info(f"Broker:             {MQTT_HOST}:{MQTT_PORT} (Client ID: {MQTT_CLIENT_ID})")
    logger.info(f"Subscription Topic: {MQTT_TOPIC} (QoS 1)")
    logger.info(f"Target S3 Endpoint: {S3_ENDPOINT_URL}")
    logger.info(f"Target S3 Bucket:   {S3_BUCKET}")
    logger.info(f"Hive Key Prefix:    {S3_PREFIX}")
    logger.info(
        f"Buffer Limits:      Max Records={BUFFER_MAX_RECORDS}, "
        f"Max Bytes={BUFFER_MAX_BYTES}B, Max Seconds={BUFFER_MAX_SECONDS}s"
    )
    logger.info("=" * 68)

    # Initialize S3 Client
    s3_client = create_s3_client()
    if not ensure_bucket_exists(s3_client, S3_BUCKET, AWS_REGION):
        logger.error("Could not initialize S3 bucket. Terminating.")
        sys.exit(1)

    # Initialize Ingestion Buffer
    buffer = IngestionBuffer(s3_client=s3_client, bucket=S3_BUCKET, prefix=S3_PREFIX)

    # Start Background Timer Worker
    stop_event = threading.Event()
    timer_thread = threading.Thread(
        target=timer_thread_fn,
        args=(buffer, stop_event),
        name="BufferTimerThread",
        daemon=True,
    )
    timer_thread.start()

    # Initialize MQTT Client
    mqtt_client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        clean_session=False,
        protocol=mqtt.MQTTv311,
    )
    on_connect, on_disconnect, on_message = create_mqtt_callbacks(buffer)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    def handle_shutdown(signum, frame):
        nonlocal mqtt_client
        global running
        signame = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        logger.info(f"Received shutdown signal {signame}. Initiating graceful shutdown...")
        running = False
        try:
            mqtt_client.disconnect()
            mqtt_client.loop_stop()
        except Exception:
            pass

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Connect to MQTT Broker
    while running:
        try:
            logger.info(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
            mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
            mqtt_client.loop_start()
            break
        except Exception as e:
            if not running:
                break
            logger.warning(f"MQTT connection failed ({e}). Retrying in 2s...")
            time.sleep(2.0)

    # Main thread waits for termination
    try:
        while running:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass

    # Graceful Shutdown
    logger.info("Executing graceful shutdown procedure...")
    stop_event.set()
    timer_thread.join(timeout=3.0)

    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass

    # Final Buffer Flush to prevent data loss
    logger.info("Flushing remaining records in buffer to S3...")
    buffer.flush(reason="graceful_shutdown")

    logger.info(
        f"Service shutdown complete. Total ingested: {buffer.total_ingested}, "
        f"Total uploaded: {buffer.total_uploaded} records."
    )


if __name__ == "__main__":
    main()
