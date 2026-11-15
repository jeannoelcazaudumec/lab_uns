================================================================================
         UNS JSON SCHEMA VALIDATION & DATA GOVERNANCE GUIDE
       (In-UNS Gatekeeper + Dead-Letter Queue in Mosquitto UNS)
================================================================================

This document explains in detail how JSON Schema Validation is architected and
implemented inside the Mosquitto UNS Unified Namespace (UNS) to protect downstream
IT databases and analytics from corrupted, malformed, or out-of-spec OT
equipment payloads.

It covers:
  1. Architectural Overview & Why Schema Governance Belongs in the UNS
  2. In-UNS Gatekeeper in `uns-enricher` + Dead-Letter Queue vs. Broker-Level Drops
  3. The Canonical JSON Schema Contract (`telemetry_schema.json`)
  4. Generating Good & Bad Records (`Schema_Fault_Simul` Flow in Node-RED)
  5. Step-by-Step Testing Guide: How to Trigger Faults and View the Messages

================================================================================
1. ARCHITECTURAL OVERVIEW: WHY VALIDATE SCHEMAS INSIDE THE UNS?
================================================================================

In an ISA-95 Unified Namespace architecture, dozens of heterogeneous edge gateways,
PLCs, and protocol converters publish data into a shared MQTT topic hierarchy.
Without strict schema governance at the UNS ingress boundary:
  * A misconfigured PLC firmware update might send `"OVERHEAT"` (a string) in a
    numeric `temperature` field, crashing downstream SQL casts or distorting
    Grafana aggregations.
  * A broken 4-20mA sensor might report `-999.9` % humidity or `9999.0` °C.
  * A rogue or uncalibrated edge flow might inject unapproved JSON fields or
    omit mandatory metadata (`asset_id`, `timestamp`).
  * A serial/Modbus frame glitch might emit truncated, non-JSON text.

By enforcing a formal JSON Schema contract inside the `it_dmz` Unified Namespace
BEFORE messages are promoted to the canonical `.../telemetry/` topic:
  1. VALID (`Good`) records are augmented with the active MES `part_number` and
     `batch_id` and published to `enterprise/plant/area/sub-area/cell/<asset>/telemetry/`.
  2. INVALID (`Bad`) records are BLOCKED from `.../telemetry/` and quarantined
     into a dedicated Dead-Letter Queue (DLQ) topic:
     `enterprise/plant/area/sub-area/cell/<asset>/dlq/` along with the exact
     field path, violated rule, and human-readable failure reason.

--------------------------------------------------------------------------------
End-to-End Schema Validation & Enrichment Data Flow
--------------------------------------------------------------------------------

  [OT Level 0 / Level 1: Edge Node-RED (`nodered` at http://localhost:1880)]
     │
     ├─► Tabs 1-3 (`Modbus 1`, `Modbus 2`, `OPC UA 1`):
     │     Continuously publish VALID raw telemetry every 5s
     │
     ├─► Tab 4 (`MES_Simul`):
     │     Publishes active `part_number` & `batch_id` (Retained, QoS 1)
     │     Topic: enterprise/plant/area/sub-area/cell/mes/state/
     │
     └─► Tab 5 (`Schema_Fault_Simul`):
           Manual buttons to inject 1 GOOD record & 5 types of BAD records
           Topic: enterprise/plant/area/sub-area/cell/asset_1/raw/
                           │
                           ▼  (Bridged via Mosquitto OT -> Mosquitto UNS in `it_dmz`)
  ┌────────────────────────────────────────────────────────────────────────────┐
  │                     Mosquitto UNS Broker (`mosquitto-uns:1883`)                   │
  └────────────────────────┬───────────────────────────────────────────────────┘
                           │
                           │ Subscribes to `.../+/raw/` and `.../mes/state/`
                           ▼
  ┌────────────────────────────────────────────────────────────────────────────┐
  │            `uns-enricher` (In-UNS Schema Gatekeeper & Enricher)            │
  │                                                                            │
  │  Step 1: Parse JSON (catch MalformedJSONError if non-JSON string)          │
  │  Step 2: Validate against `/app/schemas/telemetry_schema.json`             │
  │          (Draft 2020-12 JSON Schema via Python `jsonschema`)               │
  └──────────┬─────────────────────────────────────────────┬───────────────────┘
             │                                             │
     [PASSES SCHEMA]                               [FAILS SCHEMA]
             │                                             │
             ▼                                             ▼
  Augment with `part_number` & `batch_id`       Build Violation Report JSON
  Publish to Canonical Telemetry Topic:         Publish to Dead-Letter Queue (DLQ):
  `enterprise/.../<asset>/telemetry/`           `enterprise/.../<asset>/dlq/`
             │                                             │
             ▼                                             ▼
  ┌──────────────────────────────────────┐      ┌──────────────────────────────┐
  │ Clean Downstream IT Consumers:       │      │ Quarantine & Observability:  │
  │  • TimescaleDB (`asset_telemetry`)   │      │  • TimescaleDB (`asset_dlq`) │
  │  • DynamoDB (`asset_telemetry`)      │      │  • Grafana UNS Control Center│
  │  • Grafana Live MQTT Panels 1-3      │      │  • Grafana Live DLQ Panel 4  │
  │  • Node-RED IT (`nodered-asset3`)    │      │  • `uns-enricher` Error Logs │
  └──────────────────────────────────────┘      └──────────────────────────────┘

================================================================================
2. WHY THE `uns-enricher` GATEKEEPER + DLQ PATTERN IS IDEAL FOR MOSQUITTO UNS
================================================================================

Eclipse Mosquitto (`eclipse-mosquitto:latest`) provides lightweight, high-performance
protocol-level MQTT v3.1.1/v5 brokering,
while full JSON Schema Draft 2020-12 validation and MES context enrichment are
handled by the dedicated `uns-enricher` microservice in `it_dmz`:

| Capability                          | `uns-enricher` Gatekeeper (Implemented) | Broker-Only Drop / Disconnect           |
|-------------------------------------|-----------------------------------------|-----------------------------------------|
| Full JSON Schema Draft 2020-12      | Yes (`jsonschema` validator)            | Requires custom Java extension          |
| Blocks bad data from `telemetry/`   | Yes                                     | Yes                                     |
| Emits rich Dead-Letter Queue (`dlq`)| Yes (includes `failed_field`, `reason`) | No (silently drops or disconnects)      |
| Persists rejected rows in Timescale | Yes (`asset_dlq` audit table)           | No (dropped before reaching sinks)      |
| Combines Validation + MES Enrichment| Yes (single atomic step in `it_dmz`)    | Requires separate enricher              |
| Protects Edge-to-UNS Mosquitto Bridge | Yes (never disconnects bridge client)   | Disconnecting kills the entire bridge!  |

**Critical OT Bridge Note**: Because all OT assets are bridged into Mosquitto UNS over
a single MQTT connection (`edge-to-uns`), disconnecting a client upon a schema
violation from `asset_1` would sever the entire bridge connection for `asset_2`,
`asset_3`, and `MES_Simul`. By performing validation inside `uns-enricher`, bad
messages are cleanly quarantined to `.../dlq/` with full diagnostic context while
the OT-to-DMZ bridge remains uninterrupted.

================================================================================
3. IN-UNS GATEKEEPER & DEAD-LETTER QUEUE ARCHITECTURE
================================================================================

In our lab implementation, schema validation and MES payload augmentation are
unified inside the `uns-enricher` microservice running in the `it_dmz` network:

  * **Schema File**: `connect/uns-enricher/schemas/telemetry_schema.json`
  * **Validator/Enricher Code**: `connect/uns-enricher/enricher.py`
  * **Fault Simulator UI**: `nodered/data/flows.json` (Tab: `Schema_Fault_Simul`)
  * **TimescaleDB Quarantine Sink**: `connect/mqtt-to-timescale/sink.py` (`asset_dlq`)
  * **Grafana Dashboards**:
    - `grafana/dashboards/mosquitto_uns_inspector.json` (`4_DLQ_QUARANTINE` topic stage & KPI counter)
    - `grafana/dashboards/mosquitto_telemetry.json` (Panel 4 Live DLQ stream)

--------------------------------------------------------------------------------
3.1 The UNS Topic Contract (`raw/` vs. `telemetry/` vs. `dlq/`)
--------------------------------------------------------------------------------
Every asset in the cell has three distinct topic sub-trees in Mosquitto UNS:

  1. `enterprise/plant/area/sub-area/cell/<asset_id>/raw/`
     Untrusted, un-enriched edge output bridged from OT Mosquitto.
     Only `uns-enricher` (and `uns-inspector`) subscribes to this topic.

  2. `enterprise/plant/area/sub-area/cell/<asset_id>/telemetry/`
     Trusted, schema-validated, and MES-augmented telemetry (`part_number`,
     `batch_id`, `schema_valid: true`). Consumed by TimescaleDB (`asset_telemetry`),
     DynamoDB (`asset_telemetry`), Grafana (Panels 1-3), and `nodered-asset3`.

  3. `enterprise/plant/area/sub-area/cell/<asset_id>/dlq/`
     Dead-Letter Queue for messages that failed JSON parsing or JSON Schema
     validation. Consumed by TimescaleDB (`asset_dlq` table) and Grafana Panel 4.

================================================================================
4. THE CANONICAL JSON SCHEMA CONTRACT (`telemetry_schema.json`)
================================================================================

The file `connect/uns-enricher/schemas/telemetry_schema.json` defines the formal
JSON Schema (Draft 2020-12) enforced on every message arriving on `.../+/raw/`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://uns.enterprise.local/schemas/raw-asset-telemetry.json",
  "title": "UNS Raw Equipment Telemetry Contract",
  "description": "Strict JSON Schema governing raw OT asset payloads entering the Unified Namespace before enrichment.",
  "type": "object",
  "required": [
    "asset_id",
    "device",
    "temperature",
    "humidity",
    "timestamp"
  ],
  "properties": {
    "asset_id": {
      "type": "string",
      "pattern": "^asset_[1-9][0-9]*$",
      "description": "Canonical ISA-95 cell asset identifier (e.g., asset_1, asset_2, asset_3)"
    },
    "device": {
      "type": "string",
      "enum": ["modbus1", "modbus2", "opcua1"],
      "description": "Physical or simulated PLC/server identifier"
    },
    "protocol": {
      "type": "string",
      "enum": ["modbus-tcp", "opc-ua"]
    },
    "temperature": {
      "type": "number",
      "minimum": -40.0,
      "maximum": 150.0,
      "description": "Process temperature in degrees Celsius (-40.0 to 150.0)"
    },
    "humidity": {
      "type": "number",
      "minimum": 0.0,
      "maximum": 100.0,
      "description": "Relative humidity percentage (0.0 to 100.0)"
    },
    "motor_speed": {
      "type": "number",
      "minimum": 0.0,
      "maximum": 5000.0,
      "description": "Optional spindle/motor speed in RPM (0.0 to 5000.0)"
    },
    "raw_registers": {
      "type": "array",
      "items": {
        "type": "integer"
      }
    },
    "timestamp": {
      "type": "string",
      "minLength": 10,
      "description": "ISO-8601 UTC event timestamp"
    },
    "topic": {
      "type": "string"
    }
  },
  "additionalProperties": false
}
```

### Rules Enforced by This Schema:
1. **Mandatory Metadata (`required`)**: Every payload MUST contain `asset_id`,
   `device`, `temperature`, `humidity`, and `timestamp`.
2. **Naming Convention (`pattern` & `enum`)**: `asset_id` must match `asset_<N>`,
   and `device` must be one of the registered cell devices (`modbus1`, `modbus2`,
   `opcua1`).
3. **Strict Data Types (`type`)**: `temperature`, `humidity`, and `motor_speed`
   must be JSON numbers (`float` or `int`), never strings or booleans.
4. **Physical Sensor Bounds (`minimum` / `maximum`)**:
   * `temperature`: `-40.0` °C to `150.0` °C
   * `humidity`: `0.0` % to `100.0` %
   * `motor_speed`: `0.0` RPM to `5000.0` RPM
5. **No Rogue Fields (`additionalProperties: false`)**: Any unapproved property
   not explicitly listed in `properties` causes immediate rejection.

================================================================================
5. GENERATING GOOD & BAD RECORDS (`Schema_Fault_Simul` IN NODE-RED)
================================================================================

A 5th tab named **`Schema_Fault_Simul`** in the OT Node-RED instance
(`http://localhost:1880`) provides 6 manual **Inject** buttons that publish test
messages to `enterprise/plant/area/sub-area/cell/asset_1/raw/`:

  1. **[GOOD] Valid Asset 1 Telemetry**:
     Publishes a compliant record (`temperature: 26.5`, `humidity: 54.2`).
     -> **Result**: Passes schema validation, is augmented with `part_number`
        and `schema_valid: true`, and is published to `.../asset_1/telemetry/`.

  2. **[BAD #1] Wrong Type (`temperature` = String)**:
     Publishes `"temperature": "SENSOR_DISCONNECTED_ERR"` instead of a number.
     -> **Result**: Rejected (`failed_field: "temperature"`, `schema_rule: "type"`,
        `reason: "'SENSOR_DISCONNECTED_ERR' is not of type 'number'"`).

  3. **[BAD #2] Out-of-Range Sensor Spike (`temperature` = 9999.9)**:
     Publishes `"temperature": 9999.9` (exceeds `maximum: 150.0`).
     -> **Result**: Rejected (`failed_field: "temperature"`, `schema_rule: "maximum"`,
        `reason: "9999.9 is greater than the maximum of 150.0"`).

  4. **[BAD #3] Missing Required Field (`humidity` omitted)**:
     Publishes a payload without the mandatory `"humidity"` key.
     -> **Result**: Rejected (`failed_field: "humidity"`, `schema_rule: "required"`,
        `reason: "'humidity' is a required property"`).

  5. **[BAD #4] Unapproved Extra Property (`additionalProperties: false`)**:
     Injects `"unapproved_tag": "UNAUTHORIZED_PAYLOAD_FIELD"`.
     -> **Result**: Rejected (`failed_field: "unapproved_tag"`,
        `schema_rule: "additionalProperties"`,
        `reason: "Additional properties are not allowed ('unapproved_tag' was unexpected)"`).

  6. **[BAD #5] Corrupted Non-JSON Frame (`MalformedJSONError`)**:
     Publishes a raw corrupted string `"RAW_PLC_CRC_ERROR_0xFF00_CORRUPTED"`.
     -> **Result**: Rejected at JSON parser stage (`error_type: "MalformedJSONError"`,
        `schema_rule: "json_parse"`).

--------------------------------------------------------------------------------
Structure of a Dead-Letter Queue (`.../dlq/`) Message
--------------------------------------------------------------------------------
Whenever any of the 5 bad records is triggered, `uns-enricher` blocks it from
`.../telemetry/` and publishes the following JSON payload to
`enterprise/plant/area/sub-area/cell/asset_1/dlq/`:

```json
{
  "rejected_at": "2026-10-28T19:50:04.512301+00:00",
  "asset_id": "asset_1",
  "source_topic": "enterprise/plant/area/sub-area/cell/asset_1/raw/",
  "dlq_topic": "enterprise/plant/area/sub-area/cell/asset_1/dlq/",
  "error_type": "SchemaValidationError",
  "failed_field": "temperature",
  "schema_rule": "type",
  "reason": "'SENSOR_DISCONNECTED_ERR' is not of type 'number'",
  "active_part_number": "PN-1001-STEEL",
  "raw_payload": {
    "asset_id": "asset_1",
    "device": "modbus1",
    "temperature": "SENSOR_DISCONNECTED_ERR",
    "humidity": 55.0,
    "timestamp": "2026-10-28T19:50:04.509Z",
    "topic": "enterprise/plant/area/sub-area/cell/asset_1/raw/"
  }
}
```

================================================================================
6. HOW TO TEST AND SEE THE MESSAGES (STEP-BY-STEP GUIDE)
================================================================================

--------------------------------------------------------------------------------
Step 1: Start the Lab Stack
--------------------------------------------------------------------------------
```bash
docker compose up -d --build
```

--------------------------------------------------------------------------------
Step 2: Watch the Live `uns-enricher` Gatekeeper Logs
--------------------------------------------------------------------------------
Open a terminal and tail the `uns-enricher` logs:
```bash
docker logs -f uns-enricher
```
Every 5 seconds, you will see normal valid telemetry passing validation:
```text
[UNS VALIDATED & AUGMENTED] asset=asset_1 part_number=PN-1001-STEEL batch=LOT-2026-01 -> enterprise/plant/area/sub-area/cell/asset_1/telemetry/
```

--------------------------------------------------------------------------------
Step 3: Trigger Good & Bad Records (Via Node-RED UI or CLI)
--------------------------------------------------------------------------------
**Method A — Using the Node-RED UI (`Schema_Fault_Simul` Tab):**
  1. Open `http://localhost:1880` in your browser.
  2. Click on the **`Schema_Fault_Simul`** tab at the top.
  3. Click any of the 5 **`[BAD #1]` .. `[BAD #5]`** inject buttons (or the
     **`[GOOD]`** button).
  4. Immediately observe the debug sidebar in Node-RED and the `[SCHEMA REJECTED]`
     warnings in `docker logs -f uns-enricher`.

**Method B — Using the Command Line (Publish directly to Mosquitto OT Broker):**
You can also inject bad records directly from your terminal using `docker exec`:

  * **Inject Bad Type (`temperature` as string):**
    ```bash
    docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -q 1 \
      -t "enterprise/plant/area/sub-area/cell/asset_1/raw/" \
      -m '{"asset_id":"asset_1","device":"modbus1","temperature":"OVERHEAT_STR","humidity":50.0,"timestamp":"2026-10-28T20:00:00Z"}'
    ```

  * **Inject Out-of-Range Value (`temperature` = 9999.9):**
    ```bash
    docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -q 1 \
      -t "enterprise/plant/area/sub-area/cell/asset_1/raw/" \
      -m '{"asset_id":"asset_1","device":"modbus1","temperature":9999.9,"humidity":50.0,"timestamp":"2026-10-28T20:00:00Z"}'
    ```

  * **Inject Missing Required Field (`humidity` omitted):**
    ```bash
    docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -q 1 \
      -t "enterprise/plant/area/sub-area/cell/asset_1/raw/" \
      -m '{"asset_id":"asset_1","device":"modbus1","temperature":25.0,"timestamp":"2026-10-28T20:00:00Z"}'
    ```

  * **Inject Corrupted Non-JSON String:**
    ```bash
    docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -q 1 \
      -t "enterprise/plant/area/sub-area/cell/asset_1/raw/" \
      -m 'CORRUPTED_BINARY_FRAME_0xFF00'
    ```

--------------------------------------------------------------------------------
Step 4: Subscribe Live to the Mosquitto UNS Dead-Letter Queue (`.../+/dlq/`)
--------------------------------------------------------------------------------
To watch rejected messages arrive in real time on the Mosquitto UNS broker (`port 1884`
on host / `1883` in container):
```bash
docker compose exec mosquitto mosquitto_sub -h mosquitto-uns -p 1883 -v \
  -t "enterprise/plant/area/sub-area/cell/+/dlq/"
```
Whenever you trigger a bad record, the full DLQ JSON payload prints immediately.

--------------------------------------------------------------------------------
Step 5: Query Rejected vs. Valid Records in TimescaleDB
--------------------------------------------------------------------------------
`mqtt-to-timescale` routes valid telemetry to `asset_telemetry` and rejected
DLQ messages to `asset_dlq`.

1. **View all quarantined schema violations in `asset_dlq`:**
   ```bash
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT
     time,
     asset_id,
     error_type,
     failed_field,
     payload->>'schema_rule' AS rule,
     reason
   FROM asset_dlq
   ORDER BY time DESC
   LIMIT 10;"
   ```

2. **Verify that ZERO bad records polluted the production `asset_telemetry` table:**
   ```bash
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT
     COUNT(*) AS total_valid_rows,
     COUNT(*) FILTER (WHERE (payload->>'schema_valid')::boolean IS TRUE) AS schema_verified_rows
   FROM asset_telemetry
   WHERE time > NOW() - INTERVAL '10 minutes';"
   ```

--------------------------------------------------------------------------------
Step 6: View Live Schema Violations in Grafana (`http://localhost:3001`)
--------------------------------------------------------------------------------
1. Open `http://localhost:3001` (`admin` / `admin`).
2. Open the **Mosquitto UNS Streaming Telemetry - Direct MQTT Stream** dashboard
   (`/d/mosquitto-telemetry/mosquitto-telemetry`) or
   the **Mosquitto UNS Control Center** dashboard (`/d/mosquitto-uns-control-center/mosquitto-uns-control-center`).
3. Scroll down to **Panel 4: "Live Schema Validation — Dead-Letter Queue (`.../+/dlq/`)"**
   (or check the `4_DLQ_QUARANTINE` stage in the UNS Control Center).
4. Click the fault injection buttons in Node-RED (`http://localhost:1880` ->
   `Schema_Fault_Simul`) and watch the rejected records appear live in Panel 4
   while Panels 1–3 continue charting clean, uncorrupted telemetry!
================================================================================
