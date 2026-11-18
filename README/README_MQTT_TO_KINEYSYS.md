# Production-Grade, Cost-Optimized MQTT-to-AWS Kinesis Ingestion Pipeline
## Local Kinesis Emulation with LocalStack, 25 KB PPU Aggregation, Gzip NDJSON, & Resilient Batching

---

## 1. Executive Summary & Architecture Overview

In an **Industrial IoT (IIoT)** and **ISA-95 Unified Namespace (UNS)** architecture, real-time telemetry streams from machines, PLCs, and SCADA systems must be delivered with sub-second latency to enterprise cloud stream processing engines (such as AWS Kinesis Data Streams, Apache Flink, AWS Lambda, and Amazon Managed Service for Apache Flink).

However, **naive MQTT-to-Kinesis ingestion (putting individual MQTT messages directly into Kinesis) causes severe cloud billing overruns and operational bottlenecks**:
- **AWS Kinesis Put Payload Unit (PPU) Economics**: AWS bills Kinesis ingestion in **25 KB Put Payload Units (PPUs)**. A 400-byte telemetry message consumes an entire 25 KB PPU. A plant with 1,000 sensors sampling at 1 Hz generates **2.59 billion unoptimized PPUs per month**.
- **Shard Throttling & Hot Shards**: Static or low-cardinality partition keys cause traffic to concentrate on a single shard, triggering `ProvisionedThroughputExceededException` errors when exceeding the 1 MB/sec or 1,000 records/sec per-shard write quota.
- **Connection Handshake Overhead**: Re-establishing TCP/TLS connections per record introduces latency spikes and thread exhaustion under high message volumes.
- **Partial Batch Failures**: Naive retries of an entire `PutRecords` batch on partial failure duplicate previously written records down the analytics pipeline.

This microservice (`mqtt-to-kinesis`) implements a **production-grade, financially optimized, and resilient streaming ingestion pipeline** paired with a local AWS Kinesis emulator (**LocalStack 3.7.0**). It features:
1. **MQTT Persistent Session (`clean_session=False`, QoS 1)** for zero-loss message ingestion from the Unified Namespace.
2. **Automated Stream Provisioning** that verifies stream existence and polls until `ACTIVE`.
3. **25 KB PPU Record Aggregation** packing multiple telemetry events into a single Newline-Delimited JSON (NDJSON) record under the 25 KB boundary.
4. **Gzip Level 6 Compression** shrinking raw payloads by **65%–85%+**, packing dozens of industrial events into a single billed PPU.
5. **PutRecords Multi-Record Batching** accumulating prepared records (count, byte size, or 2.0s age trigger) to maximize write throughput.
6. **High-Cardinality Partition Keys** combining `asset_id` and random UUID entropy for uniform MD5 hash distribution across all stream shards.
7. **Connection Pooling & TCP Keep-Alive** utilizing `boto3.client('kinesis')` with a 25-connection pool.
8. **Jittered Exponential Retries for Partial Failures** inspecting `Records[].ErrorCode` to re-send *only* failed records without duplicating successful ones.
9. **Live PPU Cost Metrics** logged in real time on every flush to verify billable cost savings.
10. **Graceful Shutdown** handling `SIGINT`/`SIGTERM` to drain memory buffers cleanly before termination.

---

## 2. ISA-95 Purdue Model & Pipeline Architecture

```
 [Level 0 / Level 1: Isolated Field Devices]
  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
  │   modbus1   │      │   modbus2   │      │   opcua1    │
  │  (Asset 1)  │      │  (Asset 2)  │      │  (Asset 3)  │
  └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │ Modbus TCP / OPC UA (5s poll)
                              ▼
 [Level 2 / Level 3: OT Edge Gateway & Store-and-Forward]
  ┌────────────────────────────────────────────────────────┐
  │ nodered (Edge Gateway Container :1880)                 │
  │ mosquitto (OT Edge Broker :1883 + Persistent Bridge)   │
  └───────────────────────────┬────────────────────────────┘
                              │ Native OASIS MQTT Bridge (enterprise/#, QoS 1)
                              ▼
 [Level 3.5: Industrial DMZ (Plant UNS & Validation)]
  ┌────────────────────────────────────────────────────────┐
  │ mosquitto-uns (Mosquitto Central UNS Broker :1884)     │
  │ uns-enricher (Schema Validator & MES Context Joiner)   │
  └───────────────────────────┬────────────────────────────┘
                              │ QoS 1 Subscription (enterprise/.../+/telemetry/)
                              ▼
 [Level 4: Enterprise IT & Real-Time Streaming Tier]
  ┌────────────────────────────────────────────────────────────────────────────┐
  │ mqtt-to-kinesis Microservice (:connect/mqtt-to-kinesis)                    │
  │                                                                            │
  │  ┌──────────────────────────────────────────────────────────────────────┐  │
  │  │ Tier 1: In-Memory NDJSON Record Aggregation (<25 KB PPU)             │  │
  │  │  - Size Limit: ~24 KB (24,576 B, safely under 25,600 B PPU threshold)│  │
  │  │  - Count Limit: 50 records                                           │  │
  │  │  - Age Limit:   5.0s max window                                      │  │
  │  └──────────────────────────────────┬───────────────────────────────────┘  │
  │                                     │ Aggregation Trigger                  │
  │  ┌──────────────────────────────────▼───────────────────────────────────┐  │
  │  │ Gzip Level 6 Compression & Partition Key Generation                  │  │
  │  │  - Gzip Compression: 65%–85%+ payload reduction                      │  │
  │  │  - PartitionKey: {asset_id}_{uuid12} (Uniform MD5 Shard Spread)      │  │
  │  └──────────────────────────────────┬───────────────────────────────────┘  │
  │                                     │ Prepared Kinesis Record              │
  │  ┌──────────────────────────────────▼───────────────────────────────────┐  │
  │  │ Tier 2: PutRecords Batch Buffer & Dispatch Engine                    │  │
  │  │  - Batch Triggers: 100 records / 4 MB / 2.0s flush interval          │  │
  │  │  - Connection Pool: 25 persistent keep-alive connections             │  │
  │  │  - Selective Jittered Retry: Retries ONLY Records[].ErrorCode        │  │
  │  │  - Real-Time PPU Economics Logger                                    │  │
  │  └──────────────────────────────────┬───────────────────────────────────┘  │
  └─────────────────────────────────────┼──────────────────────────────────────┘
                                        │ PutRecords API (Port 4566)
                                        ▼
  ┌────────────────────────────────────────────────────────────────────────────┐
  │ localstack Container (AWS Kinesis Emulation :4566)                         │
  │  - Service: kinesis (Community 3.7.0)                                      │
  │  - Stream:  iiot-telemetry-stream                                          │
  │  - Region:  us-east-1                                                      │
  └────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. AWS Kinesis Cost Optimization Deep-Dive

### Financial Mechanism: 25 KB Put Payload Units (PPUs)

AWS Kinesis ingestion is billed based on **Put Payload Units (PPUs)**:
- **1 PPU = 25 KB payload chunk**.
- Any record from 1 byte to 25,600 bytes is billed as **1 PPU**.
- Any record from 25,601 bytes to 51,200 bytes is billed as **2 PPUs**.
- Standard AWS Kinesis Ingestion pricing: **\$0.014 per 1,000,000 PPUs**.

$$\text{Billable PPUs per Record} = \left\lceil \frac{\text{Payload Size in Bytes}}{25,600} \right\rceil$$

### The Naive Ingestion Pitfall

Consider an industrial site with 1,000 assets publishing 400-byte JSON telemetry once per second:

$$\text{Daily Messages} = 1,000 \text{ assets} \times 1 \text{ msg/sec} \times 86,400 \text{ sec/day} = 86,400,000 \text{ messages/day}$$
$$\text{Monthly Messages} = 86,400,000 \times 30 = 2,592,000,000 \text{ messages/month}$$

If published individually:
- Each 400-byte message uses **1 full 25 KB PPU** (wasting 25,200 bytes or 98.4% of paid bandwidth).
- **Monthly Billable PPUs**: $2,592,000,000 \text{ PPUs}$.
- **Monthly PPU Cost**: $\frac{2,592,000,000}{1,000,000} \times \$0.014 = \mathbf{\$36.29 \text{ / month}}$.
- More critically: at 1,000 msgs/sec, the plant would require at least **2 Dedicated Shards** (each shard supports max 1,000 records/sec), incurring additional shard-hour costs (\$0.015/shard-hour = \$21.60/shard/month).

### Optimization Strategy 1: NDJSON Record Aggregation

Instead of 1 message = 1 Kinesis record, `mqtt-to-kinesis` buffers incoming telemetry and packs up to 50 events into a single Newline-Delimited JSON (NDJSON) string, bounded by `AGGREGATION_MAX_BYTES=24576` (~24 KB) and `AGGREGATION_MAX_SECONDS=5.0s`.

### Optimization Strategy 2: Gzip Level 6 Compression

Industrial JSON telemetry contains repetitive schema keys (`timestamp`, `asset_id`, `temperature`, `vibration`, `part_number`). Gzip compression with level 6 achieves **65%–85%+ payload reduction**:
- **Raw NDJSON (50 events)**: $\approx 20,000 \text{ bytes}$.
- **Gzip Compressed Record**: $\approx 2,400 \text{ bytes}$.
- **Actual Billed PPUs**: $\left\lceil \frac{2,400}{25,600} \right\rceil = \mathbf{1 \text{ PPU}}$.

**Result**: 50 individual events that would have consumed 50 PPUs now consume **1 single PPU**.
$$\text{Cost Reduction} = \frac{50 - 1}{50} \times 100\% = \mathbf{98.0\% \text{ PPU Cost Savings}}$$

### Optimization Strategy 3: Multi-Record `PutRecords` API Batching

Prepared compressed records are held in a Tier 2 batch buffer and written using `boto3.client('kinesis').put_records()`, triggered when:
- Buffer reaches `BATCH_MAX_RECORDS` (e.g., 100 records).
- Buffer reaches `BATCH_MAX_BYTES` (e.g., 4 MB, well within the 5 MB AWS PutRecords limit).
- Buffer reaches `BATCH_MAX_SECONDS` (e.g., 2.0s).

This reduces HTTP request overhead, TLS negotiations, and network roundtrips by up to 100x.

### Optimization Strategy 4: High-Cardinality Partition Keys

AWS Kinesis computes the MD5 hash of `PartitionKey` to determine which shard receives the record:
$$\text{Shard Placement} = \text{MD5}(\text{PartitionKey}) \pmod{2^{128}}$$

If the partition key is static (e.g., constant `"plant"`), all records map to a single shard. If it is only `asset_id`, assets with higher event rates create hot shards.
The microservice formats partition keys as:
$$\text{PartitionKey} = \text{asset\_id} + \text{"\_"} + \text{uuid4()[:12]}$$
This guarantees a perfectly uniform distribution across the 128-bit hash space, maximizing stream throughput across all available shards.

### Optimization Strategy 5: Resilient Partial-Failure Jittered Retries

When calling `PutRecords`, Kinesis may return a `200 OK` HTTP status even if individual records inside the batch failed (e.g. `ProvisionedThroughputExceededException` or `InternalFailure`):
```python
response = kinesis.put_records(StreamName=stream, Records=batch)
if response["FailedRecordCount"] > 0:
    failed = [batch[i] for i, r in enumerate(response["Records"]) if "ErrorCode" in r]
```
The ingestion engine extracts **only the failed records** and re-submits them with exponential backoff and randomized jitter:
$$t_{\text{sleep}} = \min(t_{\text{max}}, t_{\text{base}} \times 2^{\text{attempt}-1}) + \text{Uniform}(0.05, 0.25)$$
Successfully written records are never duplicated.

---

## 4. Microservice Configuration Reference

All settings can be configured via environment variables in `docker-compose.yml`:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `MQTT_HOST` | `mosquitto-uns` | Hostname of the Central UNS MQTT broker. |
| `MQTT_PORT` | `1883` | Internal MQTT broker port. |
| `MQTT_TOPIC` | `enterprise/plant/area/sub-area/cell/+/telemetry/` | Telemetry topic pattern subscribed with QoS 1. |
| `MQTT_CLIENT_ID` | `mqtt-to-kinesis-ingestor` | Persistent MQTT client identifier (`clean_session=False`). |
| `MQTT_KEEPALIVE` | `60` | MQTT keep-alive interval in seconds. |
| `KINESIS_ENDPOINT_URL` | `http://localstack:4566` | Target AWS Kinesis endpoint (LocalStack or AWS). |
| `KINESIS_STREAM_NAME` | `iiot-telemetry-stream` | Target Kinesis Data Stream name. |
| `KINESIS_SHARD_COUNT` | `1` | Shard count used when auto-provisioning the stream. |
| `AWS_ACCESS_KEY_ID` | `test` | AWS authentication key (dummy for LocalStack). |
| `AWS_SECRET_ACCESS_KEY` | `test` | AWS secret key (dummy for LocalStack). |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS deployment region. |
| `AGGREGATION_MAX_RECORDS`| `50` | Maximum MQTT events packed into a single NDJSON record. |
| `AGGREGATION_MAX_BYTES` | `24576` | Maximum uncompressed payload bytes (~24 KB) per Kinesis record. |
| `AGGREGATION_MAX_SECONDS`| `5.0` | Maximum age in seconds before forcing record aggregation. |
| `GZIP_COMPRESS_LEVEL` | `6` | Gzip compression level (1–9). Default 6 provides optimal balance. |
| `BATCH_MAX_RECORDS` | `100` | Maximum prepared records per `PutRecords` API batch. |
| `BATCH_MAX_BYTES` | `4194304` | Maximum batch payload size (4 MB; AWS limit is 5 MB). |
| `BATCH_MAX_SECONDS` | `2.0` | Maximum delay before flushing prepared records to Kinesis. |
| `BOTO_MAX_CONNECTIONS` | `25` | Persistent HTTP connection pool size for boto3. |
| `MAX_RETRIES` | `5` | Maximum retry attempts for failed records in `PutRecords`. |

---

## 5. Verification & Testing Playbook

### 1. Launch the Pipeline

Ensure all Docker containers (including `localstack` and `mqtt-to-kinesis`) are built and started:

```bash
docker compose up -d --build
```

### 2. Verify Service Status & Healthchecks

Check that `localstack` and `mqtt-to-kinesis` are running and healthy:

```bash
docker compose ps localstack mqtt-to-kinesis
```

Inspect LocalStack healthcheck directly:
```bash
curl -s http://127.0.0.1:4566/_localstack/health | jq .
```
Expected output:
```json
{
  "services": {
    "kinesis": "available"
  },
  "edition": "community",
  "version": "3.7.0"
}
```

### 3. Observe Real-Time PPU Cost Metrics

Follow the live logs of `mqtt-to-kinesis`:

```bash
docker logs -f mqtt-to-kinesis
```

Sample live output:
```
2026-11-16 22:18:05 EST [INFO] Checking status of Kinesis stream 'iiot-telemetry-stream' at http://localstack:4566...
2026-11-16 22:18:05 EST [INFO] Stream 'iiot-telemetry-stream' not found. Initiating auto-creation (Shards=1)...
2026-11-16 22:18:06 EST [INFO] Stream 'iiot-telemetry-stream' is ACTIVE and ready (1 open shard(s)).
2026-11-16 22:18:06 EST [INFO] Connected to Mosquitto UNS broker at mosquitto-uns:1883 (rc=0)
2026-11-16 22:18:06 EST [INFO] Subscribed with QoS 1 to validated telemetry topic: 'enterprise/plant/area/sub-area/cell/+/telemetry/'
...
2026-11-16 22:18:15 EST [INFO] [PPU METRICS] Flush #1: 6 events -> 1 Kinesis record(s) | Raw: 2,148 B -> Gzip: 432 B (79.9% byte savings) | PPUs: 6 unoptimized -> 1 billed (83.3% cost reduction) | Cumulative: 6 events, 6 unopt PPUs vs 1 billed PPUs (83.3% total cost savings, 79.9% byte savings)
2026-11-16 22:18:25 EST [INFO] [PPU METRICS] Flush #2: 6 events -> 1 Kinesis record(s) | Raw: 2,154 B -> Gzip: 435 B (79.8% byte savings) | PPUs: 6 unoptimized -> 1 billed (83.3% cost reduction) | Cumulative: 12 events, 12 unopt PPUs vs 2 billed PPUs (83.3% total cost savings, 79.8% byte savings)
```

### 4. Run the End-to-End Stream Verification Script

Execute the included `verify_stream.py` tool inside the container:

```bash
docker exec -it mqtt-to-kinesis python verify_stream.py
```

Sample output:
```
===========================================================================
MQTT-to-AWS Kinesis Stream Ingestion Verification Tool
Endpoint: http://localstack:4566 | Stream: iiot-telemetry-stream | Region: us-east-1
===========================================================================
[1/5] Stream Status: 'ACTIVE' with 1 open shard(s).
[2/5] Discovered 1 shard(s): shardId-000000000000
[3/5] Polling records from shards (Iterator: TRIM_HORIZON)...
      Shard 'shardId-000000000000': retrieved 3 Kinesis record(s).
[4/5] Ingestion & Payload Verification Results:
      - Kinesis Records Fetched:      3
      - Telemetry Events Parsed:      18
      - Total Raw (NDJSON) Bytes:     6,450 bytes
      - Total Gzip Compressed Bytes:  1,302 bytes
      - Gzip Compression Ratio:       79.81% data reduction
[5/5] AWS Kinesis PPU Financial Economics:
      - Unoptimized PPUs (1/event):   18 PPUs
      - Actual Billed PPUs (25KB):    3 PPUs
      - Billable Ingestion Savings:   83.33% COST REDUCTION

---------------------------------------------------------------------------
Sample Decompressed UNS Telemetry Event:
{
  "asset_id": "asset_1",
  "timestamp": "2026-11-16T22:18:20.123456Z",
  "part_number": "PN-1001-STEEL",
  "batch_id": "LOT-2026-01",
  "temperature": 74.2,
  "vibration": 0.045
}
---------------------------------------------------------------------------

>>> ALL VERIFICATION CHECKS PASSED SUCCESSFULLY! <<<
```

### 5. Verify Graceful Shutdown & Buffer Drain

Test buffer drain and clean exit by stopping the container:

```bash
docker compose stop mqtt-to-kinesis
docker logs mqtt-to-kinesis --tail 25
```

The logs will display the shutdown sequence:
```
[INFO] Received SIGTERM. Initiating graceful shutdown...
[INFO] Stopping MQTT client network loop...
[INFO] Clean MQTT disconnection completed.
[INFO] Draining in-memory aggregation and batch buffers...
[INFO] Flushing final 1 record(s) to Kinesis...
[INFO] ===========================================================================
[INFO] FINAL INGESTION & COST SAVINGS SUMMARY
[INFO] Total Flushes:              3
[INFO] Total Telemetry Events:     18
[INFO] Kinesis Records Written:    3
[INFO] Uncompressed Data:          6,450 bytes
[INFO] Compressed Data Written:    1,302 bytes
[INFO] Total Bytes Saved:          5,148 bytes (79.8%)
[INFO] Unoptimized PPUs (1/msg):   18 PPUs
[INFO] Actual Billed PPUs (25KB):  3 PPUs
[INFO] Total PPUs Saved:           15 PPUs (83.3% billable cost savings)
[INFO] ===========================================================================
[INFO] MQTT-to-Kinesis ingestion service exited cleanly.
```

Restart the service when ready:
```bash
docker compose up -d mqtt-to-kinesis
```

---

## 6. Financial & Operational Comparison Matrix

| Metric / Dimension | Naive Direct Kinesis Ingestion | Optimized `mqtt-to-kinesis` Service | Advantage |
| :--- | :--- | :--- | :--- |
| **Record Aggregation** | 1 Event = 1 Record | Up to 50 Events / NDJSON Record | **98% Fewer Records** |
| **Payload Compression** | Uncompressed raw JSON | Gzip level 6 compression | **65%–85%+ Payload Reduction** |
| **PPU Utilization** | ~400 B per 25 KB PPU (1.6% efficiency) | ~24 KB raw compressed into 1 PPU | **>95% PPU Packing Efficiency** |
| **PPU Ingestion Cost** | 1 PPU billed per event | 1 PPU billed per 50 events | **>80%–98% Billable Cost Reduction** |
| **Shard Requirements** | High (1 shard per 1,000 msgs/sec) | Low (1 shard supports 50,000 msgs/sec) | **Up to 50x Fewer Shards Needed** |
| **Shard Distribution** | Risk of hot shards from static keys | Uniform MD5 distribution via UUIDs | **Zero Hot Shards** |
| **HTTP Request Volume** | 1 API call per message | 1 `PutRecords` batch per 100 records | **100x Fewer API Calls & Handshakes** |
| **Network Pooling** | Ephemeral connections per request | 25 persistent TCP Keep-Alive sockets | **Sub-millisecond API Latency** |
| **Partial Failure Handling**| Re-sends whole batch (duplicates data) | Retries *only* failed records with jitter | **Zero Duplicate Records** |
| **Graceful Shutdown** | Messages in transit lost on restart | Full buffer drain on `SIGINT`/`SIGTERM` | **Zero Message Loss Guarantee** |
