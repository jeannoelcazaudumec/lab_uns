================================================================================
TWO-TIER MOSQUITTO UNIFIED NAMESPACE (UNS) PAYLOAD AUGMENTATION & CONTEXTUALIZATION GUIDE
================================================================================

1. EXECUTIVE SUMMARY & INDUSTRIAL USE CASE
--------------------------------------------------------------------------------
In real-world manufacturing environments (ISA-95 / Purdue Model), low-level
field sensors and machine controllers (`modbus1`, `modbus2`, `opcua1` at Level 0)
only measure raw physical parameters—such as Temperature (°C), Humidity (%RH),
and Motor Speed (RPM). They have no awareness of the business or manufacturing
execution context, such as:
  * Which **Part Number (`part_number`)** is currently being produced on the line.
  * Which **Production Lot / Batch (`batch_id`)** is active.

Meanwhile, a Line PLC or Manufacturing Execution System (**`MES_Simul`**) issues
discrete state events whenever the production line starts producing a new part
number.

This laboratory demonstrates **Real-Time In-UNS Payload Augmentation**:
1. A simulated PLC/MES flow (**`MES_Simul`** in Node-RED) publishes a retained
   production state event (`part_number`, `batch_id`) into the Unified Namespace.
2. The 3 industrial assets (`asset_1`, `asset_2`, `asset_3`) publish their raw
   sensor readings to `.../raw/` topics.
3. Inside the **Mosquitto Central Unified Namespace (`it_dmz` network)**, the **`uns-enricher`**
   service joins the active `part_number` and `batch_id` onto every raw telemetry
   message from all 3 assets in real time and publishes the augmented payload to
   `.../telemetry/`.
4. All downstream IT historians and applications (`timescaledb`, `dynamodb-local`,
   `grafana`, `nodered-asset3`) consume the enriched `.../telemetry/` stream,
   allowing engineers to correlate machine health and process parameters directly
   against specific part numbers.


================================================================================
2. END-TO-END AUGMENTATION ARCHITECTURE & TOPIC TAXONOMY
================================================================================

### 2.1 Data Flow Diagram
```
  [OT Edge: nodered (:1880)]
     │
     ├── Flow 1 (tab_modbus1)        ──> enterprise/.../cell/asset_1/raw/ (QoS 1, Every 5s)
     ├── Flow 2 (tab_modbus2)        ──> enterprise/.../cell/asset_2/raw/ (QoS 1, Every 5s)
     ├── Flow 3 (tab_opcua1)         ──> enterprise/.../cell/asset_3/raw/ (QoS 1, Every 5s)
     ├── Flow 4 (MES_Simul)          ──> enterprise/.../cell/mes/state/   (QoS 1, RETAIN=TRUE)
     └── Flow 5 (Schema_Fault_Simul) ──> enterprise/.../cell/asset_1/raw/ (Good/Bad Test Records)
                                           │
                                           ▼
                       [OT Edge Broker: mosquitto (:1883)]
                       (Native Bridge: connection edge-to-uns, topic enterprise/# out 1)
                                           │
                                           ▼
                     [Plant UNS Broker: mosquitto-uns (:1883 / :8000)]
                     (Stores Retained MES State + Routes Streams)
                        │                                  ▲
   1. Delivers Retained │                                  │ 2a. Valid Schema: Publishes Enriched JSON
      .../mes/state/    │                                  │     (+part_number, +batch_id, +schema_valid)
      & Live .../+/raw/ ▼                                  │     to .../+/telemetry/
                     [UNS Gatekeeper & Enricher: uns-enricher (it_dmz)]
                                                           │ 2b. Invalid Schema: Publishes Violation Report
                                                           │     to .../+/dlq/ (Dead-Letter Queue)
     ┌─────────────────────────────────────────────────────┴──────────────────────┐
     ▼                                  ▼                                         ▼
[mqtt-to-timescale]         [connect-uns-to-dynamodb]           [grafana & nodered-asset3]
     │                                  │                                         │
     ▼                                  ▼                                         ▼
[timescaledb (:5432)]       [dynamodb-local (:8000)]               [Live Dashboards (:3001/:1881)]
(`asset_telemetry`,         (Top-level `part_number`               (UNS Control Center, Live MQTT
 `asset_dlq` & `mosquitto-uns_*`)   attribute + full `payload` map)        Stream & Asset 3 Consumer)
```

### 2.2 Complete UNS Topic Taxonomy

| Topic Path                                                     | Publisher      | Subscriber(s)               | QoS | Retain | Purpose |
|----------------------------------------------------------------|----------------|-----------------------------|-----|--------|---------|
| `enterprise/plant/area/sub-area/cell/mes/state/`               | `nodered` (`MES_Simul`) | `uns-enricher`     | `1` | `true` | Holds the active `part_number` and `batch_id` as persistent broker state in Mosquitto UNS. |
| `enterprise/plant/area/sub-area/cell/asset_1/raw/`             | `nodered` (`tab_modbus1`, `Schema_Fault_Simul`)| `uns-enricher` | `1` | `false`| Raw un-enriched temperature & humidity from `modbus1` (plus fault test payloads). |
| `enterprise/plant/area/sub-area/cell/asset_2/raw/`             | `nodered` (`tab_modbus2`)| `uns-enricher`    | `1` | `false`| Raw un-enriched temperature & humidity from `modbus2`. |
| `enterprise/plant/area/sub-area/cell/asset_3/raw/`             | `nodered` (`tab_opcua1`) | `uns-enricher`    | `1` | `false`| Raw un-enriched temp, humidity & motor speed from `opcua1`. |
| `enterprise/plant/area/sub-area/cell/asset_1/telemetry/`       | `uns-enricher` | `mqtt-to-timescale`, `connect-uns-to-dynamodb`, `grafana` | `1` | `false` | Schema-validated, augmented Asset 1 telemetry (`part_number`, `batch_id`, `schema_valid: true`). |
| `enterprise/plant/area/sub-area/cell/asset_2/telemetry/`       | `uns-enricher` | `mqtt-to-timescale`, `connect-uns-to-dynamodb`, `grafana` | `1` | `false` | Schema-validated, augmented Asset 2 telemetry (`part_number`, `batch_id`, `schema_valid: true`). |
| `enterprise/plant/area/sub-area/cell/asset_3/telemetry/`       | `uns-enricher` | `mqtt-to-timescale`, `connect-uns-to-dynamodb`, `grafana`, `nodered-asset3` | `1` | `false` | Schema-validated, augmented Asset 3 telemetry (`part_number`, `batch_id`, `schema_valid: true`). |
| `enterprise/plant/area/sub-area/cell/+/dlq/`                   | `uns-enricher` | `mqtt-to-timescale`, `grafana` | `1` | `false` | Dead-Letter Queue for payloads failing JSON Schema validation (`telemetry_schema.json`). |


================================================================================
3. COMPONENT 1: THE `"MES_Simul"` FLOW IN NODE-RED (`nodered/data/flows.json`)
================================================================================

### 3.1 How `"MES_Simul"` Works
In the Edge Node-RED instance (`http://localhost:1880`), the 4th flow tab is named
**`MES_Simul`** (`id: "tab_mes_simul"`). It simulates a Line PLC / MES controller
announcing production order changes.

It provides two ways to trigger a part number change:
1. **Automatic Cycling (`inject_mes_auto` - "Auto Rotate Part # (Every 60s)")**:
   Fires 1 second after startup and repeats every 60 seconds, rotating through
   three part numbers in order:
   - `PN-1001-STEEL`    (`batch_id: "LOT-2026-01"`)
   - `PN-2002-ALUM`     (`batch_id: "LOT-2026-02"`)
   - `PN-3003-TITANIUM` (`batch_id: "LOT-2026-03"`)
2. **Manual Demo Buttons (`inject_mes_part1`, `inject_mes_part2`, `inject_mes_part3`)**:
   Three manual inject nodes in the `MES_Simul` tab allow an operator or presenter
   to click any button in the Node-RED browser editor (`http://localhost:1880`)
   to immediately switch the active part number on demand:
   - `Start Part: PN-1001-STEEL`
   - `Start Part: PN-2002-ALUM`
   - `Start Part: PN-3003-TITANIUM`

### 3.2 Complete JavaScript Code in `func_mes_simul`
```javascript
const parts = [
  { part_number: 'PN-1001-STEEL', batch_id: 'LOT-2026-01' },
  { part_number: 'PN-2002-ALUM', batch_id: 'LOT-2026-02' },
  { part_number: 'PN-3003-TITANIUM', batch_id: 'LOT-2026-03' }
];

let selected;
if (msg.payload === 'AUTO') {
  let idx = context.get('part_idx') || 0;
  selected = parts[idx % parts.length];
  context.set('part_idx', (idx + 1) % parts.length);
} else {
  selected = parts.find(p => p.part_number === msg.payload) || {
    part_number: String(msg.payload),
    batch_id: 'LOT-MANUAL'
  };
}

const outTopic = 'enterprise/plant/area/sub-area/cell/mes/state/';
msg.topic = outTopic;
msg.payload = {
  source: 'MES_Simul',
  plc_id: 'plc_cell_1',
  event: 'PRODUCTION_STARTED',
  part_number: selected.part_number,
  batch_id: selected.batch_id,
  timestamp: new Date().toISOString(),
  topic: outTopic
};

node.status({
  fill: 'blue',
  shape: 'dot',
  text: 'Producing: ' + selected.part_number + ' (' + selected.batch_id + ')'
});
return msg;
```

### 3.3 Why `retain: "true"` Is Used on `mqtt_out_mes_simul`
Production part changes are **state transitions** (they happen once every few
minutes or hours), whereas sensor telemetry is **continuous** (every 5 seconds).
By publishing the `MES_Simul` message with `retain: "true"`, both `mosquitto` and
`mosquitto-uns` store the last known MES state on `enterprise/plant/area/sub-area/cell/mes/state/`.
If `uns-enricher` restarts at any time, `mosquitto-uns` immediately delivers the retained
`MES_Simul` message upon connection so `uns-enricher` never loses track of the
active `part_number`.


================================================================================
4. COMPONENT 2: THE IN-UNS CONTEXT ENRICHER (`uns-enricher`)
================================================================================

### 4.1 Architectural Placement (`it_dmz` Network)
`uns-enricher` runs inside the `it_dmz` network directly alongside `mosquitto-uns`. It has
no connection to the `level0` field network or the `it` database network—its sole
responsibility is **in-UNS schema validation and stream augmentation**.

### 4.2 Docker Compose Configuration (`docker-compose.yml`)
```yaml
  uns-enricher:
    build:
      context: ./connect/uns-enricher
      dockerfile: Dockerfile
    image: nodered-uns-enricher:latest
    container_name: uns-enricher
    restart: unless-stopped
    environment:
      - MQTT_HOST=mosquitto-uns
      - MQTT_PORT=1883
      - MQTT_CLIENT_ID=uns-enricher
      - MES_STATE_TOPIC=enterprise/plant/area/sub-area/cell/mes/state/
      - RAW_TELEMETRY_TOPIC=enterprise/plant/area/sub-area/cell/+/raw/
    networks:
      - it_dmz
    depends_on:
      - mosquitto-uns
```

### 4.3 Source Code Overview (`connect/uns-enricher/enricher.py`)
`connect/uns-enricher/enricher.py` connects to `mosquitto-uns:1883` (`clean_session=False`,
`client_id="uns-enricher"`), subscribes to `MES_STATE_TOPIC` and `RAW_TELEMETRY_TOPIC`,
validates incoming raw payloads against `telemetry_schema.json` (Draft 2020-12),
enriches compliant payloads with `part_number`, `batch_id`, and `schema_valid: true`
on `.../telemetry/`, and quarantines non-compliant payloads to `.../dlq/`.


================================================================================
5. PAYLOAD WALKTHROUGH: BEFORE vs. AFTER AUGMENTATION
================================================================================

### A. MES Production Event Published by `MES_Simul`
* Topic: `enterprise/plant/area/sub-area/cell/mes/state/` (`QoS 1`, `Retain: true`)
```json
{
  "source": "MES_Simul",
  "plc_id": "plc_cell_1",
  "event": "PRODUCTION_STARTED",
  "part_number": "PN-2002-ALUM",
  "batch_id": "LOT-2026-02",
  "timestamp": "2026-10-28T19:56:15.102Z",
  "topic": "enterprise/plant/area/sub-area/cell/mes/state/"
}
```

### B. Raw Equipment Telemetry Arriving at Mosquitto UNS (Before Augmentation)
* Topic: `enterprise/plant/area/sub-area/cell/asset_3/raw/`
```json
{
  "asset_id": "asset_3",
  "device": "opcua1",
  "protocol": "opc-ua",
  "temperature": 26.87,
  "humidity": 53.7,
  "motor_speed": 1509.7,
  "timestamp": "2026-10-28T19:56:21.060Z",
  "topic": "enterprise/plant/area/sub-area/cell/asset_3/raw/"
}
```

### C. Enriched Telemetry Published in Mosquitto Central UNS (After Augmentation)
* Topic: `enterprise/plant/area/sub-area/cell/asset_3/telemetry/`
```json
{
  "asset_id": "asset_3",
  "device": "opcua1",
  "protocol": "opc-ua",
  "temperature": 26.87,
  "humidity": 53.7,
  "motor_speed": 1509.7,
  "timestamp": "2026-10-28T19:56:21.060Z",
  "topic": "enterprise/plant/area/sub-area/cell/asset_3/telemetry/",
  "part_number": "PN-2002-ALUM",
  "batch_id": "LOT-2026-02",
  "schema_valid": true
}
```


================================================================================
6. DOWNSTREAM IT CONSUMPTION OF AUGMENTED DATA
================================================================================

1. TimescaleDB (`mqtt-to-timescale` -> `timescaledb`):
   - Subscribes to `enterprise/plant/area/sub-area/cell/+/telemetry/`.
   - Inserts the augmented JSON into `asset_telemetry(time, payload)`.
   - Because `payload` is a `JSONB` column with a `GIN` index (`idx_asset_telemetry_payload`),
     `part_number` and `batch_id` are immediately queryable via SQL (`payload->>'part_number'`)
     with zero table migrations.

2. DynamoDB Local (`connect-uns-to-dynamodb` -> `dynamodb-local`):
   - Redpanda Connect (`connect/mosquitto-to-dynamodb.yaml`) subscribes to
     `enterprise/plant/area/sub-area/cell/+/telemetry/` on `mosquitto-uns:1883`.
   - Extracts `root.part_number = this.part_number.or("UNKNOWN")`.
   - Logs every record with its part number:
     `Streaming from Mosquitto UNS to DynamoDB: asset=asset_3 part=PN-2002-ALUM device=opcua1 ...`
   - Writes `part_number` both as a top-level string attribute (`S`) on the
     DynamoDB item and inside the nested `payload` map (`M`).

3. Grafana (`grafana` on `:3001`) & Asset 3 App (`nodered-asset3` on `:1881`):
   - Grafana's `Mosquitto Central UNS Control Center` dashboard (`mosquitto-uns_uns_inspector.json`)
     shows the `3_MES_CONTEXT` retained topic and the enriched `2_VALIDATED_TELEMETRY`
     payloads live in the Topics table.
   - Grafana's `Mosquitto UNS Streaming Telemetry` dashboard (`mosquitto-uns_telemetry.json`) Panel 3
     subscribes to `enterprise/plant/area/sub-area/cell/+/telemetry/` and displays
     the `part_number` and `batch_id` columns live in the browser.
   - `nodered-asset3` (`http://localhost:1881`) displays the active `part_number`
     on the `Process Asset 3 Payloads` node status badge (`Asset 3: 26.87°C | 1509.7 RPM | Part: PN-2002-ALUM`).


================================================================================
7. STEP-BY-STEP LIVE DEMONSTRATION & VERIFICATION COMMANDS
================================================================================

Step 1: Open the Node-RED `"MES_Simul"` Tab in Your Browser
-----------------------------------------------------------
1. Navigate to: http://localhost:1880
2. Click the **`MES_Simul`** tab at the top of the flow editor.
3. Click the inject button on any of the manual triggers:
   - `Start Part: PN-1001-STEEL`
   - `Start Part: PN-2002-ALUM`
   - `Start Part: PN-3003-TITANIUM`
   (Or wait 60 seconds for the `Auto Rotate Part #` timer to cycle automatically).

Step 2: Watch the UNS Context Enricher Logs in Real Time
--------------------------------------------------------
  docker compose logs -f uns-enricher

Example Output:
  [INFO] [MES CONTEXT UPDATED] Active Part Number: PN-2002-ALUM | Batch: LOT-2026-02
  [INFO] [UNS VALIDATED & AUGMENTED] asset=asset_1 part_number=PN-2002-ALUM batch=LOT-2026-02 -> enterprise/plant/area/sub-area/cell/asset_1/telemetry/
  [INFO] [UNS VALIDATED & AUGMENTED] asset=asset_2 part_number=PN-2002-ALUM batch=LOT-2026-02 -> enterprise/plant/area/sub-area/cell/asset_2/telemetry/
  [INFO] [UNS VALIDATED & AUGMENTED] asset=asset_3 part_number=PN-2002-ALUM batch=LOT-2026-02 -> enterprise/plant/area/sub-area/cell/asset_3/telemetry/

Step 3: Query TimescaleDB Analytics Grouped by Part Number & Asset
------------------------------------------------------------------
  docker compose exec timescaledb psql -U postgres -d telemetry -c "
  SELECT
    payload->>'part_number' AS part_number,
    payload->>'batch_id' AS batch_id,
    payload->>'asset_id' AS asset,
    ROUND(AVG((payload->>'temperature')::numeric), 2) AS avg_temp_c,
    ROUND(AVG((payload->>'humidity')::numeric), 2) AS avg_humidity_pct,
    ROUND(AVG((payload->>'motor_speed')::numeric), 1) AS avg_motor_rpm,
    COUNT(*) AS samples
  FROM asset_telemetry
  WHERE payload ? 'part_number'
  GROUP BY 1, 2, 3
  ORDER BY 1, 3;"

Step 4: Query Augmented Items in DynamoDB Local
-----------------------------------------------
  python3 dynamodb/query_table.py

Example Output:
  === DynamoDB Table: asset_telemetry | Total Items: 2670 (Status: ACTIVE) ===
  [2026-10-28T19:49:45.623Z] asset=asset_2 part=PN-2002-ALUM device=modbus2 temp=25.45°C hum=54.46% rpm=-
  [2026-10-28T19:49:40.623Z] asset=asset_2 part=PN-2002-ALUM device=modbus2 temp=25.9°C hum=54.85% rpm=-
================================================================================
