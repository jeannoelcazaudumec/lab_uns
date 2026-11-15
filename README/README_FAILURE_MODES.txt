================================================================================
INDUSTRIAL IOT LAB: FAILURE MODES, BACKPRESSURE & DATA LOSS ANALYSIS (MOSQUITTO UNS)
================================================================================

1. OVERVIEW OF THE MULTI-TIER STORE-AND-FORWARD ARCHITECTURE
--------------------------------------------------------------------------------
The laboratory is engineered with two independent 50,000-message store-and-forward
buffers and end-to-end QoS 1 transactional backpressure across its segmented
Purdue / ISA-95 networks (`level0`, `edge_ot_it`, `it_dmz`, `it`):

* Emission Rate:
  3 industrial assets (`modbus1`, `modbus2`, `opcua1`) polled every 5 seconds
  = 3 messages / 5 seconds = 0.6 messages/sec = 2,160 messages/hour.

* Tier 1 Buffer — OT Edge Broker (`mosquitto` on `edge_ot_it` + `it_dmz`):
  - Configured in `mosquitto/config/mosquitto.conf` with `persistence true`,
    `persistence_location /mosquitto/data/`, `max_queued_messages 50000`, and
    native outbound bridge `connection edge-to-uns` (`cleansession false`,
    `try_private false`, `notifications false`, `topic enterprise/# out 1`).
  - Protects against: `mosquitto-uns` downtime or WAN/DMZ network outages for ~23.1 hours.

* Tier 2 Buffer — Plant UNS Broker (`mosquitto-uns` on `it_dmz`):
  - Configured in `mosquitto-uns/config/mosquitto.conf` with `persistence true`,
    `persistence_location /mosquitto/data/`, `max_queued_messages 50000`, and
    `persistent_client_expiration 7d`.
  - Holds the latest `MES_Simul` production state (`part_number`, `batch_id`) as
    an MQTT **Retained Message** on `enterprise/plant/area/sub-area/cell/mes/state/`.
  - Maintains independent persistent session queues (`clean_session=false`, QoS 1)
    for each UNS processor and downstream IT consumer:
      1. `uns-enricher`                (validates schema & augments `.../raw/` -> `.../telemetry/` or `.../dlq/`)
      2. `timescaledb-group`           (used by `mqtt-to-timescale` for `.../telemetry/` & `.../dlq/`)
      3. `redpanda-connect-dynamodb`   (used by `connect-uns-to-dynamodb`)
      4. `nodered-asset3-consumer-app` (used by `nodered-asset3`)
  - Protects against: Enricher, connector, or database outages for ~23.1 hours.


================================================================================
2. SCENARIO 1: `mqtt-to-timescale` SERVICE STOPS
================================================================================
Question: What happens if `mqtt-to-timescale` stops? Do I lose messages?
Answer:   NO messages are lost (for outages up to ~23.1 hours).

Step-by-Step Behavior:
1. Persistent Session Queuing in Mosquitto UNS:
   In `connect/mqtt-to-timescale/sink.py`, the sink connects to `mosquitto-uns:1883` using:
     - `client_id = "timescaledb-group"`
     - `clean_session = False`
     - `qos = 1` on topics `enterprise/plant/area/sub-area/cell/+/telemetry/` and `enterprise/plant/area/sub-area/cell/+/dlq/`
   When `mqtt-to-timescale` stops, `mosquitto-uns` keeps the subscription for
   `timescaledb-group` active and queues every incoming enriched QoS 1 message
   (`asset_1`, `asset_2`, `asset_3`, validated against `telemetry_schema.json` and
   augmented with `part_number` by `uns-enricher`, as well as any Dead-Letter Queue
   violation events on `.../+/dlq/`) up to 50,000 messages (~23.1 hours) on disk
   in `/mosquitto/data/mosquitto.db`.

2. In-Flight Message Protection:
   `TransactionalMQTTClient` in `sink.py` sets `max_inflight_messages_set(1)` and
   withholds the MQTT `PUBACK` until the PostgreSQL `INSERT` succeeds. If
   `mqtt-to-timescale` is stopped mid-processing before committing to TimescaleDB,
   `mosquitto-uns` never receives the `PUBACK` and retains that message at the head of the
   queue for redelivery.

3. Recovery & Chronological Replay:
   When `mqtt-to-timescale` starts again, it reconnects as `timescaledb-group`
   (`clean_session=False`). `mosquitto-uns` immediately flushes the queued backlog in
   chronological order. Because `sink.py` extracts the original `"timestamp"`
   generated at the OT Edge by `nodered` (rather than using database insertion
   time), TimescaleDB reconstructs the historical timeline with zero gaps.


================================================================================
3. SCENARIO 2: `timescaledb` STOPS (WHILE `mqtt-to-timescale` STAYS RUNNING)
================================================================================
Question: What happens if the `timescaledb` database stops?
Answer:   NO messages are lost (for outages up to ~23.1 hours).

Step-by-Step Behavior:
1. Database Write Failure & PUBACK Suppression:
   - `mosquitto-uns` delivers the next QoS 1 message to `mqtt-to-timescale`.
   - `insert_telemetry()` in `sink.py` attempts to execute:
     `INSERT INTO asset_telemetry (time, payload) VALUES (%s, %s::jsonb);`
   - Because `timescaledb:5432` is down, the PostgreSQL connection fails.
   - Unlike standard MQTT clients that send `PUBACK` before running the message
     callback, `TransactionalMQTTClient` suppresses automatic `PUBACK` generation:
     `Withholding PUBACK for mid=<id> from Mosquitto UNS (TimescaleDB unavailable)`
   - `mqtt-to-timescale` disconnects its MQTT session without acknowledging the
     message and enters `ensure_db_connection()`, retrying `timescaledb:5432` every 3s.

2. Upstream Buffering in Mosquitto UNS:
   - Because the in-flight message was never `PUBACK`ed, `mosquitto-uns` keeps it at the
     head of the `timescaledb-group` session queue and buffers all subsequent
     enriched plant messages (up to 50,000 messages / ~23.1 hours).

3. Complete Isolation of Other Plant Applications:
   - `uns-enricher`: Continues augmenting raw payloads with `part_number`.
   - `connect-uns-to-dynamodb` -> `dynamodb-local`: Continues streaming and
     storing NoSQL records normally.
   - `nodered-asset3`: Continues updating the Asset 3 dashboard on port `1881`.
   - `grafana`:
     * SQL Dashboards (`modbus_telemetry.json` and `mosquitto_uns_inspector.json`)
       show a temporary PostgreSQL connection error while `timescaledb` is offline.
     * Live MQTT Dashboard continues rendering real-time telemetry directly from
       `mosquitto-uns:1883` uninterrupted.

4. Automatic Recovery When `timescaledb` Restarts:
   - `ensure_db_connection()` in `mqtt-to-timescale` succeeds as soon as
     `timescaledb:5432` is ready.
   - `mqtt-to-timescale` reconnects to `mosquitto-uns:1883` (`timescaledb-group`), receives
     the unacknowledged message followed by the entire queued backlog, inserts
     them with their original edge timestamps, and sends `PUBACK`s to `mosquitto-uns`.


================================================================================
4. SCENARIO 3: `connect-uns-to-dynamodb` FAILS OR STOPS
================================================================================
Question: What happens if `connect-uns-to-dynamodb` fails?
Answer:   NO messages are lost (for outages up to ~23.1 hours).

Step-by-Step Behavior:
1. Dedicated Persistent Session in Mosquitto UNS (`redpanda-connect-dynamodb`):
   In `connect/mosquitto-to-dynamodb.yaml`, the MQTT input is configured with:
     - `client_id: "redpanda-connect-dynamodb"`
     - `clean_session: false`
     - `qos: 1`
     - `auto_replay_nacks: true`
   While `connect-uns-to-dynamodb` is stopped, `mosquitto-uns` buffers all incoming
   `enterprise/plant/area/sub-area/cell/+/telemetry/` messages in the dedicated
   `redpanda-connect-dynamodb` session queue (up to 50,000 messages / ~23.1 hours),
   completely independent of `timescaledb-group`.

2. Container Self-Healing:
   In `docker-compose.yml`, `connect-uns-to-dynamodb` is configured with
   `restart: unless-stopped`, so the Docker daemon automatically restarts the
   container if the process crashes unexpectedly.

3. Automatic Drain on Recovery:
   When `connect-uns-to-dynamodb` reconnects to `mosquitto-uns:1883`, `mosquitto-uns` drains the
   queued backlog through Redpanda Connect into `dynamodb-local:8000`. Because the
   DynamoDB Sort Key (`RANGE`) is mapped to the original edge payload timestamp
   (`timestamp: "${!json(\"timestamp\")}"`), all delayed items are stored in
   `asset_telemetry` under their exact original sensor polling timestamps.


================================================================================
5. SCENARIO 4: `dynamodb-local` FAILS (WHILE `connect-uns-to-dynamodb` RUNS)
================================================================================
Question: What happens if `dynamodb-local` fails while `connect-uns-to-dynamodb`
          stays running?
Answer:   NO messages are lost (for outages up to ~23.1 hours), provided
          `connect-uns-to-dynamodb` remains running until `dynamodb-local` recovers.

Step-by-Step Behavior:
1. `aws_dynamodb` Output NACK & `auto_replay_nacks` Retry Loop:
   - `connect-uns-to-dynamodb` pulls a message from `mosquitto-uns:1883`, runs the Bloblang
     `mapping` and `log` processors, and attempts an HTTP `PutItem` request to
     `http://dynamodb-local:8000`.
   - Because `dynamodb-local` is down, the HTTP request fails (`connection refused`
     or `i/o timeout`) and the output returns an internal NACK.
   - Because `auto_replay_nacks: true` is enabled in `connect/mosquitto-to-dynamodb.yaml`,
     Redpanda Connect does NOT drop the failed message. Instead, it holds the
     message in memory and retries the write to `dynamodb-local:8000` with
     exponential backoff.

2. Pipeline Backpressure into Mosquitto UNS:
   - Redpanda Connect uses an unbuffered internal transaction channel between its
     `input` and `output`.
   - While `output.aws_dynamodb` is blocked retrying the current in-flight message,
     `connect-uns-to-dynamodb` stops reading new messages from its `input.mqtt`
     channel.
   - Once the in-flight window fills up, TCP/MQTT backpressure causes `mosquitto-uns` to
     buffer all subsequent incoming messages in the `redpanda-connect-dynamodb`
     session queue (up to 50,000 messages).

3. Recovery When `dynamodb-local` Restarts:
   - Because `dynamodb-local` runs with `-sharedDb -dbPath /home/dynamodblocal/data`
     mounted to `./dynamodb/data`, the `asset_telemetry` table and all previously
     stored items persist across container restarts.
   - As soon as `dynamodb-local:8000` comes back online, `connect-uns-to-dynamodb`'s
     next retry succeeds (`HTTP 200 OK`), unblocking the pipeline and draining the
     entire backlog from `mosquitto-uns` in chronological order.


================================================================================
6. SCENARIO 5: `uns-enricher` STOPS OR RESTARTS
================================================================================
Question: What happens if `uns-enricher` stops or restarts? Does it lose raw
          telemetry or forget the current `part_number`?
Answer:   NO messages are lost, and the active `part_number` is restored
          immediately upon reconnect.

Step-by-Step Behavior:
1. Retained MES State in Mosquitto UNS:
   The `MES_Simul` flow in `nodered` publishes `part_number` events to
   `enterprise/plant/area/sub-area/cell/mes/state/` with **`retain: true`**.
   Therefore, `mosquitto-uns` always stores the latest active `part_number` on disk and
   delivers it immediately whenever `uns-enricher` connects or restarts.
2. Persistent Session Queuing (`client_id="uns-enricher"`):
   `uns-enricher` connects with `clean_session=False` and `qos=1` on
   `enterprise/plant/area/sub-area/cell/+/raw/`. While `uns-enricher` is stopped,
   `mosquitto-uns` buffers up to 50,000 raw asset messages (~23.1 hours) and flushes them
   through `uns-enricher` upon recovery.


================================================================================
7. SCENARIO 6: `mosquitto-uns` STOPS OR EDGE-TO-DMZ NETWORK LINK FAILS
================================================================================
Question: What happens if the central Plant UNS (`mosquitto-uns`) stops or the `it_dmz`
          network link between the OT Edge and the DMZ is severed?
Answer:   NO messages are lost (for outages up to ~23.1 hours), and even a full
          power loss on either the OT Edge or Mosquitto UNS preserves queues on disk.

Step-by-Step Behavior:
1. Uninterrupted Edge Polling:
   `nodered` continues polling `modbus1`, `modbus2`, and `opcua1` every 5 seconds
   over `level0` and publishing QoS 1 messages (along with `MES_Simul` events)
   to `mosquitto` over `edge_ot_it`.
2. Disk-Backed Edge Queuing (`mosquitto.db`):
   `mosquitto` immediately acknowledges `nodered` (`PUBACK`) and queues all
   unacknowledged outbound messages for `connection edge-to-uns` (`topic enterprise/# out 1`,
   `cleansession false`) in RAM and flushes them to `/mosquitto/data/mosquitto.db`
   every 60 seconds (up to `max_queued_messages 50000`, ~23.1 hours).
3. Automatic Bridge Reconnection:
   When `mosquitto-uns` comes back online, `mosquitto` reconnects (`restart_timeout 2 10`)
   and drains the entire edge backlog to `mosquitto-uns`, where `uns-enricher` augments
   the payloads and fans them out to `mqtt-to-timescale`, `connect-uns-to-dynamodb`,
   `grafana`, and `nodered-asset3`.


================================================================================
8. SUMMARY MATRIX & DIAGNOSTIC COMMANDS
================================================================================

| Failed Component            | Buffer Location                                 | Max Buffer Capacity       | Data Loss? |
|-----------------------------|-------------------------------------------------|---------------------------|------------|
| `uns-enricher`              | `mosquitto-uns` (`uns-enricher` queue + MES)    | 50,000 msgs (~23.1 hours) | None       |
| `mqtt-to-timescale`         | `mosquitto-uns` (`timescaledb-group` queue)     | 50,000 msgs (~23.1 hours) | None       |
| `timescaledb`               | `mosquitto-uns` (`timescaledb-group` queue)     | 50,000 msgs (~23.1 hours) | None       |
| `connect-uns-to-dynamodb`   | `mosquitto-uns` (`redpanda-connect` queue)      | 50,000 msgs (~23.1 hours) | None       |
| `dynamodb-local`            | `connect-uns-to-dynamodb` RAM + `mosquitto-uns` | 50,000 msgs (~23.1 hours) | None*      |
| `nodered-asset3`            | `mosquitto-uns` (`nodered-asset3-consumer-app`) | 50,000 msgs (~23.1 hours) | None       |
| `mosquitto-uns` / DMZ Link  | `mosquitto` (`/mosquitto/data/mosquitto.db`)    | 50,000 msgs (~23.1 hours) | None       |

*(Note: Both Mosquitto edge and central brokers persist their session queues and
retained messages to disk in `/mosquitto/data/mosquitto.db`).*

Useful Commands to Monitor Clients, Subscriptions & Queues During a Failure:
----------------------------------------------------------------------------
1. Inspect Mosquitto UNS native client connection events:
   tail -f mosquitto-uns/log/mosquitto.log

2. Inspect UNS client and subscription status in TimescaleDB:
   docker compose exec timescaledb psql -U postgres -d telemetry \
     -c "SELECT client_id, status, ip_address, clean_start, messages_in, messages_out, last_seen FROM uns_clients;" \
     -c "SELECT subscription_id, client_id, client_status, routed_messages, last_delivery FROM uns_subscriptions;"

3. Watch `uns-enricher`, `mqtt-to-timescale`, or `connect-uns-to-dynamodb` logs:
   docker compose logs -f uns-enricher
   docker compose logs -f mqtt-to-timescale
   docker compose logs -f connect-uns-to-dynamodb
================================================================================
