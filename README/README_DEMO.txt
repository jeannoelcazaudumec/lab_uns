================================================================================
        UNIFIED NAMESPACE (UNS) INDUSTRIAL IoT LAB — MASTER DEMO GUIDE
     Step-by-Step Presenter Script: Commands, Web UIs, Data Augmentation,
           JSON Schema Validation, Multi-Sink Storage & Fault Tolerance
                         (Two-Tier Eclipse Mosquitto UNS)
================================================================================

This guide provides a complete, chronological, presenter-ready walkthrough to
demonstrate the entire 14-container Industrial IoT Unified Namespace (UNS) lab
powered by **Two-Tier Eclipse Mosquitto (`eclipse-mosquitto:latest`)**.

--------------------------------------------------------------------------------
QUICK REFERENCE: WEB INTERFACES & CREDENTIALS
--------------------------------------------------------------------------------
| Application / Service                  | URL                                                | Credentials           |
|----------------------------------------|----------------------------------------------------|-----------------------|
| Edge Node-RED (OT Gateway)             | http://localhost:1880                              | None                  |
| Grafana Dashboards & UNS Control Center| http://localhost:3001                              | `admin` / `admin`     |
|   • Mosquitto Central UNS Control Center| http://localhost:3001/d/mosquitto-uns-control-center/mosquitto-uns-control-center | `admin` / `admin` |
|   • Mosquitto Central UNS Direct MQTT Stream| http://localhost:3001/d/mosquitto-telemetry/mosquitto-telemetry | `admin` / `admin` |
|   • Plant Telemetry (TimescaleDB SQL)  | http://localhost:3001/d/modbus-telemetry/plant-telemetry-modbus-and-opc-ua-sensors | `admin` / `admin` |
| Asset 3 Consumer App (IT Layer)        | http://localhost:1881                              | None                  |
| Mosquitto Central UNS Broker (MQTT/WS) | `localhost:1884` (TCP) / `ws://localhost:8083`     | Anonymous             |
| TimescaleDB (PostgreSQL)               | `localhost:5432` (`telemetry`)                     | `postgres`/`postgres` |
| DynamoDB Local (NoSQL)                 | http://localhost:8000                              | `local` / `local`     |

===============================================================================
PART 0: LAUNCHING THE LAB & VERIFYING PURDUE NETWORK SEGMENTATION
================================================================================

Step 0.1 — Start All 14 Containers
----------------------------------
From the root directory of the repository, run:

```bash
docker compose up -d --build
```

Step 0.2 — Verify All Containers Are Running
--------------------------------------------
```bash
docker compose ps -a
```
*What to point out:*
  * All 13 long-running services show `Up`, and `dynamodb-init` shows
    `Exited (0)` after provisioning the `asset_telemetry` NoSQL table.

Step 0.3 — Show the 4 Isolated Purdue / ISA-95 Docker Networks
--------------------------------------------------------------
```bash
docker network ls --filter name="level0|edge_ot_it|it_dmz|it"
```
*What to point out:*
  1. `level0`: Field network isolating PLC/equipment simulators (`modbus1`, `modbus2`, `opcua1`).
  2. `edge_ot_it`: OT Edge network connecting `nodered` to the local `mosquitto` broker.
  3. `it_dmz`: Plant DMZ hosting the central **Mosquitto UNS broker** (`mosquitto-uns`) and the
     **UNS Schema Gatekeeper & Context Enricher** (`uns-enricher`).
  4. `it`: Enterprise IT network hosting **TimescaleDB**, **DynamoDB Local**,
     **Grafana**, and downstream consumer apps.

Step 0.4 — Prove Network Isolation (Field Devices Cannot Reach IT)
------------------------------------------------------------------
Run a quick ping test proving the OT Edge Gateway can reach `modbus1` on `level0`
and `mosquitto` on `edge_ot_it`, but CANNOT directly reach `mosquitto-uns` or `timescaledb`:

```bash
docker compose exec nodered sh -c "ping -c 1 modbus1 && ping -c 1 mosquitto"
docker compose exec nodered sh -c "ping -c 1 -W 2 timescaledb || echo 'BLOCKED: OT Edge cannot directly reach IT Database!'"
```

================================================================================
ACT 1: OT FIELD SIMULATION & EDGE GATEWAY (LEVEL 0 -> LEVEL 2)
================================================================================

Step 1.1 — Visit the OT Edge Node-RED Flow Editor
-------------------------------------------------
* **Open in Browser**: http://localhost:1880
* **What to show across the first 3 tabs**:
  1. **Tab 1 (`Modbus 1 - Temp & Humidity`)**:
     Polls `modbus1:502` Holding Registers every 5 seconds, extracts `temperature`
     and `humidity`, and publishes raw JSON to:
     `enterprise/plant/area/sub-area/cell/asset_1/raw/`
  2. **Tab 2 (`Modbus 2 - Temp & Humidity`)**:
     Polls `modbus2:502` every 5 seconds and publishes raw JSON to:
     `enterprise/plant/area/sub-area/cell/asset_2/raw/`
  3. **Tab 3 (`OPC UA 1 - Temp, Humidity & Speed`)**:
     Polls `opc.tcp://opcua1:4840` (`ns=2;s=Temperature`, `Humidity`, `MotorSpeed`)
     every 5 seconds and publishes raw JSON to:
     `enterprise/plant/area/sub-area/cell/asset_3/raw/`

* **Click the Debug Icon** (bug icon in the right-hand sidebar of Node-RED):
  Show the raw payloads being emitted every 5 seconds. Note that these raw edge
  payloads do **NOT** contain a `part_number` yet—field sensors only know raw
  physics (`temperature`, `humidity`, `motor_speed`).

Step 1.2 — Show the Raw Edge Stream Arriving at Mosquitto
---------------------------------------------------------
In your terminal, subscribe to `.../+/raw/` on the OT Edge Mosquitto broker for
a few seconds (`-C 3` exits automatically after 3 messages):

```bash
docker compose exec mosquitto mosquitto_sub -h localhost -p 1883 -v -C 3 \
  -t "enterprise/plant/area/sub-area/cell/+/raw/"
```

================================================================================
ACT 2: THE UNIFIED NAMESPACE IN MOSQUITTO UNS (`it_dmz` LAYER)
================================================================================

Step 2.1 — Visit the UNS Control Center Dashboard in Grafana
------------------------------------------------------------
* **Open in Browser**:
  http://localhost:3001/d/mosquitto-uns-control-center/mosquitto-uns-control-center
* **Login**: Username `admin` / Password `admin`.
* **What to point out on the Control Center dashboard**:
  1. **Row 1 (Broker KPIs)**: Shows `UNS Broker Status: ONLINE (:1883)`,
     `Connected Clients`, `Persistent Sessions (clean=false)`, `Active UNS Topics`,
     `Active Subscriptions`, `Retained Topics`, `Total UNS Messages In`, and
     `DLQ Quarantined Msgs`.
  2. **Row 2 (Throughput & Bandwidth Graphs)**: Live 5-second message throughput
     stacked by UNS stage (`1_RAW_OT`, `2_VALIDATED_TELEMETRY`, `3_MES_CONTEXT`,
     `4_DLQ_QUARANTINE`) and byte bandwidth per topic.
  3. **Row 3 (`UNS Clients & Persistent Sessions Inspector`)**:
     Shows all connected MQTT clients:
     * `edge-to-uns` (OT Edge Mosquitto Bridge Producer)
     * `uns-enricher` (In-UNS Schema Gatekeeper & MES Enricher)
     * `timescaledb-group` (TimescaleDB Transactional Historian Sink)
     * `redpanda-connect-dynamodb` (Redpanda Connect -> DynamoDB NoSQL Sink)
     * `nodered-asset3-consumer-app` (Node-RED Asset 3 Plant Consumer App)
  4. **Row 4 (`UNS Active Subscriptions & Wildcard Routing Inspector`)**:
     Shows every active subscription filter, QoS, session queue policy (`50,000 msgs`),
     matched active topics count, and live routed message/byte counters.
  5. **Row 5 (`UNS Topic Tree & Latest Payload Inspector`)**:
     Displays the complete ISA-95 topic hierarchy, stage badge, retained flag,
     message counts, and the **Latest JSON Payload Preview** on each topic.
  6. **Row 6 (`Native Client Connection & Session Audit Log`)**:
     Live table of client connect/disconnect events parsed directly from
     `/broker-log/mosquitto.log`.

Step 2.2 — Inspect Connected Clients, Subscriptions & Event Log via CLI
-----------------------------------------------------------------------
```bash
tail -n 10 mosquitto-uns/log/mosquitto.log
docker compose exec timescaledb psql -U postgres -d telemetry \
  -c "SELECT client_id, status, role, ip_address, clean_start, messages_in, messages_out FROM uns_clients ORDER BY clean_start, client_id;" \
  -c "SELECT subscription_id, client_id, client_status, topic_filter, matched_topics_count, routed_messages FROM uns_subscriptions ORDER BY subscription_id;"
```

================================================================================
ACT 3: IN-UNS DATA AUGMENTATION (JOINING MES `part_number` WITH ASSET TELEMETRY)
================================================================================

*Narrative:* "Our 3 industrial machines only report temperature, humidity, and
motor speed. Meanwhile, a Line PLC / MES tells the cell which `part_number` is
currently being manufactured. Inside the Mosquitto UNS, `uns-enricher` joins the
active `part_number` and `batch_id` onto every asset's telemetry in real time."

Step 3.1 — Watch the `uns-enricher` Logs in a Terminal
------------------------------------------------------
Open a terminal window and stream the live `uns-enricher` logs:

```bash
docker compose logs -f --tail=15 uns-enricher
```
You will see every raw asset payload being validated and augmented with the
current `part_number` and published to `.../telemetry/`:
```text
[INFO] [UNS VALIDATED & AUGMENTED] asset=asset_1 part_number=PN-1001-STEEL batch=LOT-2026-01 -> enterprise/plant/area/sub-area/cell/asset_1/telemetry/
```

Step 3.2 — Compare `.../raw/` vs. `.../telemetry/` Side by Side
---------------------------------------------------------------
Run this command in another terminal to capture 1 raw message and 1 augmented
telemetry message from Mosquitto Central UNS:

```bash
echo "=== 1. RAW PAYLOAD FROM EDGE (.../asset_3/raw/) ===" && \
docker compose exec mosquitto mosquitto_sub -h mosquitto-uns -p 1883 -C 1 \
  -t "enterprise/plant/area/sub-area/cell/asset_3/raw/" && \
echo "" && \
echo "=== 2. AUGMENTED PAYLOAD IN UNS (.../asset_3/telemetry/) ===" && \
docker compose exec mosquitto mosquitto_sub -h mosquitto-uns -p 1883 -C 1 \
  -t "enterprise/plant/area/sub-area/cell/asset_3/telemetry/"
```
*What to point out:*
  The `.../telemetry/` payload now includes:
  * `"part_number": "PN-1001-STEEL"`
  * `"batch_id": "LOT-2026-01"`
  * `"schema_valid": true`

Step 3.3 — Trigger a Live Part Number Change in Node-RED (`MES_Simul` Tab)
--------------------------------------------------------------------------
1. **Open in Browser**: http://localhost:1880
2. Click on the 4th tab: **`MES_Simul`**.
3. Point out the 4 inject nodes:
   * `Auto Rotate Part # (Every 60s)` (automatically cycles parts every minute)
   * `Start Part: PN-1001-STEEL`
   * `Start Part: PN-2002-ALUM`
   * `Start Part: PN-3003-TITANIUM`
4. **Click the button next to `Start Part: PN-3003-TITANIUM`**.
   * Point out the blue status badge under the function node:
     `Producing: PN-3003-TITANIUM (LOT-2026-03)`
   * Point out in your `docker compose logs -f uns-enricher` terminal that
     `uns-enricher` immediately logged:
     `[MES CONTEXT UPDATED] Active Part Number: PN-3003-TITANIUM | Batch: LOT-2026-03`
     and all 3 assets (`asset_1`, `asset_2`, `asset_3`) immediately switched to
     tagging their telemetry with `PN-3003-TITANIUM`!

Step 3.4 — Verify the New Part Number Live in Downstream Web UIs
----------------------------------------------------------------
1. **Visit the IT Asset 3 Consumer App**:
   * **Open in Browser**: http://localhost:1881
   * Look at the `Process Asset 3 Payloads` node status badge—it now shows:
     `Asset 3: <temp>°C | <rpm> RPM | Part: PN-3003-TITANIUM`
2. **Visit Grafana Live Streaming Dashboard**:
   * **Open in Browser**: http://localhost:3001 (Login: `admin` / `admin`)
   * Go to **Dashboards -> Mosquitto Central UNS Streaming Telemetry - Direct MQTT Stream**
     (`http://localhost:3001/d/mosquitto-telemetry/mosquitto-telemetry`)
   * Look at **Panel 3 (`Live Enriched Telemetry Stream (With Active Part Number)`)**:
     Show `part_number` (`PN-3003-TITANIUM`) and `batch_id` (`LOT-2026-03`)
     updating live in the table.

================================================================================
ACT 4: IN-UNS JSON SCHEMA VALIDATION & DEAD-LETTER QUEUE (`.../dlq/`)
================================================================================

*Narrative:* "What happens if a PLC firmware glitch or broken sensor sends a
string instead of a number for temperature, a 9999.9°C spike, a missing field,
an unauthorized JSON property, or corrupted non-JSON text? `uns-enricher`
validates every message on `.../+/raw/` against a strict Draft 2020-12 JSON Schema
(`telemetry_schema.json`) BEFORE it can reach `.../telemetry/`. Valid messages
flow to `.../telemetry/`; invalid messages are blocked and quarantined to the
Dead-Letter Queue topic `.../+/dlq/`."

Step 4.1 — Inspect the Canonical JSON Schema Contract
-----------------------------------------------------
```bash
cat connect/uns-enricher/schemas/telemetry_schema.json
```
*What to point out:*
  * `required`: `["asset_id", "device", "temperature", "humidity", "timestamp"]`
  * `temperature`: must be `number` between `-40.0` and `150.0`
  * `humidity`: must be `number` between `0.0` and `100.0`
  * `additionalProperties: false` (blocks any unapproved fields)

Step 4.2 — Open Live DLQ Listeners (Terminal + Browser)
-------------------------------------------------------
1. **In a Terminal**, start a live MQTT subscriber on the Central UNS Dead-Letter
   Queue topic (`.../+/dlq/`):
   ```bash
   docker compose exec mosquitto mosquitto_sub -h mosquitto-uns -p 1883 -v \
     -t "enterprise/plant/area/sub-area/cell/+/dlq/"
   ```
2. **In Grafana (`http://localhost:3001`)**, scroll down on the **Central UNS Streaming
   Telemetry** dashboard to **Panel 4 (`Live Schema Validation — Dead-Letter Queue (.../+/dlq/)`)**.

Step 4.3 — Inject Good & Bad Records from Node-RED (`Schema_Fault_Simul` Tab)
-----------------------------------------------------------------------------
1. **Open in Browser**: http://localhost:1880
2. Click on the 5th tab: **`Schema_Fault_Simul`**.
3. Click each of the 6 buttons one by one and watch the Node-RED status badge,
   the `mosquitto_sub` terminal, `uns-enricher` logs, and Grafana Panel 4:

   * **Button 1: `[GOOD] Valid Asset 1 Telemetry`**
     -> Node-RED status turns **green** (`Sent GOOD compliant record`).
     -> Passes schema validation and goes to `.../asset_1/telemetry/`. Nothing
        is sent to `.../dlq/`.

   * **Button 2: `[BAD #1] Wrong Type (temperature = String)`**
     -> Sends `"temperature": "SENSOR_DISCONNECTED_ERR"`.
     -> Blocked from `.../telemetry/`! Routed to `.../asset_1/dlq/` with:
        `failed_field: "temperature"`, `schema_rule: "type"`,
        `reason: "'SENSOR_DISCONNECTED_ERR' is not of type 'number'"`.

   * **Button 3: `[BAD #2] Out-of-Range Spike (temp = 9999.9)`**
     -> Sends `"temperature": 9999.9` (exceeds `maximum: 150.0`).
     -> Blocked and routed to `.../asset_1/dlq/` with:
        `failed_field: "temperature"`, `schema_rule: "maximum"`,
        `reason: "9999.9 is greater than the maximum of 150.0"`.

   * **Button 4: `[BAD #3] Missing Required Field (no humidity)`**
     -> Omits `"humidity"`.
     -> Blocked and routed to `.../asset_1/dlq/` with:
        `failed_field: "humidity"`, `schema_rule: "required"`,
        `reason: "'humidity' is a required property"`.

   * **Button 5: `[BAD #4] Unapproved Extra Property`**
     -> Adds `"unapproved_tag": "UNAUTHORIZED_PAYLOAD_FIELD"`.
     -> Blocked and routed to `.../asset_1/dlq/` with:
        `failed_field: "unapproved_tag"`, `schema_rule: "additionalProperties"`,
        `reason: "Additional properties are not allowed ('unapproved_tag' was unexpected)"`.

   * **Button 6: `[BAD #5] Corrupted Non-JSON Frame`**
     -> Sends raw text `"RAW_PLC_CRC_ERROR_0xFF00_CORRUPTED"`.
     -> Blocked and routed to `.../asset_1/dlq/` with:
        `error_type: "MalformedJSONError"`, `schema_rule: "json_parse"`.

*(Tip: You can also trigger all 6 buttons via CLI if desired:)*
```bash
docker compose exec nodered node -e "
const ids = ['inject_schema_good','inject_schema_bad_type','inject_schema_bad_range','inject_schema_bad_missing','inject_schema_bad_extra','inject_schema_bad_json'];
(async () => { for (const id of ids) { await fetch('http://localhost:1880/inject/' + id, { method: 'POST' }); await new Promise(r => setTimeout(r, 300)); } })();
"
```

Step 4.4 — Query Quarantined Schema Violations in TimescaleDB (`asset_dlq`)
---------------------------------------------------------------------------
Show that `mqtt-to-timescale` recorded every rejected message in the `asset_dlq`
audit hypertable while keeping `asset_telemetry` 100% clean:

```bash
docker compose exec timescaledb psql -U postgres -d telemetry -c "
SELECT time,
       asset_id,
       error_type,
       failed_field,
       payload->>'schema_rule' AS rule,
       reason
FROM asset_dlq
ORDER BY time DESC
LIMIT 10;"
```

================================================================================
ACT 5: MULTI-SINK IT PERSISTENCE (TIMESCALEDB & DYNAMODB LOCAL)
================================================================================

Step 5.1 — Query TimescaleDB Analytics Grouped by `part_number` & `asset_id`
----------------------------------------------------------------------------
Demonstrate how storing the augmented `part_number` inside TimescaleDB allows
instant manufacturing analytics by part number and batch:

```bash
docker compose exec timescaledb psql -U postgres -d telemetry -c "
SELECT payload->>'part_number' AS part_number,
       payload->>'batch_id' AS batch_id,
       payload->>'asset_id' AS asset,
       ROUND(AVG((payload->>'temperature')::numeric), 2) AS avg_temp_c,
       ROUND(AVG((payload->>'humidity')::numeric), 2) AS avg_hum_pct,
       ROUND(AVG((payload->>'motor_speed')::numeric), 1) AS avg_motor_rpm,
       COUNT(*) AS valid_samples
FROM asset_telemetry
WHERE payload ? 'part_number'
GROUP BY 1, 2, 3
ORDER BY 1, 3;"
```

Step 5.2 — Visit the Grafana Historical SQL Dashboard
-----------------------------------------------------
* **Open in Browser**:
  http://localhost:3001/d/modbus-telemetry/plant-telemetry-modbus-and-opc-ua-sensors
* Show the live stat panels and historical time-series graphs queried directly
  from TimescaleDB for `modbus1`, `modbus2`, and `opcua1`.

Step 5.3 — Inspect Redpanda Connect Streaming to AWS DynamoDB Local
-------------------------------------------------------------------
1. Check the live Redpanda Connect stream logs:
   ```bash
   docker compose logs --tail=10 connect-uns-to-dynamodb
   ```
2. Query the `asset_telemetry` NoSQL table in DynamoDB Local using the included
   query script:
   ```bash
   python3 dynamodb/query_table.py
   ```
   *(Or via Docker on the `it` network if running inside a restricted shell:)*
   ```bash
   docker run --rm --network it -e DYNAMODB_ENDPOINT=http://dynamodb-local:8000 \
     -v $(pwd)/dynamodb/query_table.py:/app/query_table.py:ro \
     python:3.11-slim python /app/query_table.py
   ```

================================================================================
ACT 6: STORE-AND-FORWARD RESILIENCE & ZERO DATA LOSS DEMO
================================================================================

*Narrative:* "What happens if our IT database (`timescaledb`) goes offline for
maintenance or crashes? Does the factory lose telemetry?"

Step 6.1 — Stop `timescaledb` to Simulate an IT Database Outage
---------------------------------------------------------------
```bash
docker compose stop timescaledb
```

Step 6.2 — Show `mqtt-to-timescale` Withholding `PUBACK` & Mosquitto UNS Queuing Messages
----------------------------------------------------------------------------------------
Wait ~10 seconds while the equipment continues polling, then run:

```bash
docker compose logs --tail=10 mqtt-to-timescale
tail -n 10 mosquitto-uns/log/mosquitto.log
```
*What to point out:*
  1. `mqtt-to-timescale` detects that `timescaledb` is unreachable, withholds
     MQTT `PUBACK` (`Withholding PUBACK for mid=... from Mosquitto UNS`), and
     disconnects cleanly so Mosquitto UNS buffers the unacknowledged message and
     all subsequent messages in its persistent disk queue (`max_queued_messages: 50000`
     / ~23.1 hours).
  2. Meanwhile, `connect-uns-to-dynamodb` and Grafana live MQTT streaming
     continue running with zero impact!

Step 6.3 — Restart `timescaledb` and Watch the Backlog Drain Automatically
--------------------------------------------------------------------------
```bash
docker compose start timescaledb
sleep 6
docker compose logs --tail=15 mqtt-to-timescale
```
*What to point out:*
  * `mqtt-to-timescale` reconnects automatically, drains all queued messages into
    TimescaleDB with their **original edge timestamps**, and **zero messages are lost**.

================================================================================
PART 7: STOPPING THE LAB AFTER THE DEMO
================================================================================

To cleanly stop all 14 containers while preserving all database volumes and
configurations:

```bash
docker compose stop
```
================================================================================
