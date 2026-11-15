# Industrial IoT Store-and-Forward (SaF) Architecture Guide
## Implementation with Node-RED & Mosquitto vs. Direct In-Flow Methods

In an **Industrial IoT (IIoT)** and **ISA-95 Unified Namespace (UNS)** architecture, edge gateways collect mission-critical telemetry across field buses (Modbus TCP, OPC UA) and transmit it across plant network tiers to a centralized message broker. 

Because plant networks and WAN links experience transient packet loss, switch reboots, maintenance windows, and fiber cuts, edge nodes must implement **Store-and-Forward (SaF)**: buffering data locally during network outages and re-transmitting it with original timestamps upon reconnection without data loss or message reordering.

---

## 1. The Implemented Architecture: Node-RED + Co-Located Mosquitto Broker

In this laboratory, Store-and-Forward is implemented using a **Two-Tier Broker Architecture**, pairing Node-RED with a dedicated, lightweight local **Eclipse Mosquitto** broker sidecar.

### Topology Diagram

```
 [Level 0 / Level 1: Field Network]
  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
  │   modbus1   │      │   modbus2   │      │   opcua1    │
  │  (Asset 1)  │      │  (Asset 2)  │      │  (Asset 3)  │
  └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │ Modbus TCP / OPC UA (5s poll)
                              ▼
 [Level 2 / Level 3: OT Edge Gateway Node]
  ┌────────────────────────────────────────────────────────┐
  │ nodered (Edge Gateway Container :1880)                 │
  │ - Polls equipment every 5 seconds                      │
  │ - Serializes canonical JSON telemetry payloads         │
  │ - Publishes QoS 1 to local localhost broker            │
  └───────────────────────────┬────────────────────────────┘
                              │ MQTT QoS 1 (tcp://mosquitto:1883)
                              ▼
  ┌────────────────────────────────────────────────────────┐
  │ mosquitto (OT Edge Broker Container :1883)             │
  │ - Tier 1 Store-and-Forward Engine                      │
  │ - Disk Persistence: /mosquitto/data/mosquitto.db       │
  │ - Max Queue: 50,000 persistent QoS 1 messages          │
  │ - Native Outbound Bridge: connection edge-to-uns       │
  └───────────────────────────┬────────────────────────────┘
                              │
               ═══════════════╪═══════════════ (Network Partition / Outage Boundary)
                              │ Outbound OASIS Bridge (QoS 1)
                              ▼
 [Level 3.5: Industrial DMZ]
  ┌────────────────────────────────────────────────────────┐
  │ mosquitto-uns (Central Plant UNS Broker :1884, WS:8083)│
  │ - Tier 2 Store-and-Forward for downstream IT consumers │
  │ - Retained MES work order production context           │
  └────────────────────────────────────────────────────────┘
```

---

### How It Works: Step-by-Step

#### 1. Data Collection & Local Handshake (Normal Operation)
- Node-RED polls field simulators every 5s via Modbus TCP and OPC UA.
- Node-RED's MQTT Client node connects to the local Edge broker (`mosquitto:1883`) over the internal Docker network (`edge_ot_it`).
- Node-RED publishes telemetry payloads to `.../raw/` with **QoS 1** (At least once delivery).
- Because `mosquitto` is on the same host, the TCP round-trip latency for the MQTT `PUBACK` is sub-millisecond (< 1 ms). Node-RED never blocks, experiences no backpressure, and never accumulates memory buffers.

#### 2. Native Edge-to-DMZ Bridging
- The local Edge Mosquitto broker runs an outbound bridge defined in [mosquitto/config/mosquitto.conf](mosquitto/config/mosquitto.conf):
  ```text
  listener 1883
  allow_anonymous true
  persistence true
  persistence_location /mosquitto/data/
  autosave_interval 60
  max_queued_messages 50000
  persistent_client_expiration 7d

  # Native OT-to-DMZ Outbound Bridge
  connection edge-to-uns
  address mosquitto-uns:1883
  remote_clientid edge-to-uns
  topic enterprise/# out 1
  cleansession false
  try_private false
  notifications false
  bridge_protocol_version mqttv311
  bridge_attempt_unsubscribe false
  restart_timeout 2 10
  ```
- With `cleansession false` and `topic enterprise/# out 1`, the central broker (`mosquitto-uns`) registers a persistent session for `edge-to-uns`.
- Under normal conditions, every message received by `mosquitto` is immediately bridged over the DMZ conduit to `mosquitto-uns`.

#### 3. Network Outage & Store Phase
- If `mosquitto-uns` stops or the network connection between OT and DMZ drops:
  1. The bridge detects TCP disconnection (via TCP RST/FIN or MQTT keepalive timeout).
  2. Node-RED **continues polling without interruption**. Its connection to `localhost:1883` remains connected and healthy.
  3. `mosquitto` acknowledges every QoS 1 message from Node-RED with a `PUBACK`, preventing Node-RED from queueing or dropping messages.
  4. `mosquitto` stores outgoing bridged messages in its internal queue in RAM and periodically flushes state to `/mosquitto/data/mosquitto.db` (enforced by `autosave_interval 60` and graceful shutdown handlers).
  5. **Buffer Capacity**: Configured with `max_queued_messages 50000`. At the baseline lab emission rate (3 assets polled every 5s = 0.6 msgs/sec = 2,160 msgs/hr), this provides:
     $$\text{Autonomy} = \frac{50{,}000 \text{ messages}}{0.6 \text{ msgs/sec}} = 83{,}333 \text{ seconds} \approx \mathbf{23.1 \text{ hours of zero data loss}}.$$
  6. If the entire edge gateway container host reboots or loses power during the outage, `mosquitto.db` on the volume mount persists the queue.

#### 4. Reconnection & Forward Phase
- As soon as network connectivity to `mosquitto-uns` is restored:
  1. The bridge reconnects automatically (retrying between 2 and 10 seconds).
  2. It resumes the persistent MQTT session (`cleansession false`).
  3. It streams the buffered messages in **strict FIFO sequence** to `mosquitto-uns`.
  4. Each message retains its **original creation timestamp** generated by the edge gateway.
  5. As `mosquitto-uns` returns `PUBACK` acknowledgments, `mosquitto` safely evicts the transmitted records from its disk store.

---

## 2. Alternative Methods: Implementing Store-and-Forward Directly in Node-RED

If your hardware or organizational policy precludes running a co-located broker like Mosquitto, Store-and-Forward can be built directly inside Node-RED flows using one of four design patterns:

```
                            DIRECT NODE-RED STORAGE PATTERNS
                            
   ┌────────────────────────────────────────────────────────┐
   │ 1. In-Memory Queue   : flow.get('queue') array (RAM)   │
   │ 2. File Spooling     : append to JSONL disk file       │
   │ 3. Embedded SQLite   : local transactional SQL table   │
   │ 4. Contrib SaF Nodes : node-red-contrib-store-forward  │
   └────────────────────────────────────────────────────────┘
```

---

### Method A: In-Memory Flow Context Queue with Status Gate

The simplest pure Node-RED approach uses flow or global context variables in RAM combined with a `Status` node watching the MQTT client node.

#### Architecture
```
 [Sensor Poll] ──► [Switch: Is Online?] ──Yes──► [MQTT Out: Central UNS]
                          │ (No)
                          ▼
                   [Function: Push to Array]
                          ▲
   [Status: Broker] ──► [Function: On Connect, Drain Array with Delay]
```

#### Implementation Example
1. **Connection Monitoring Function**:
   ```javascript
   // Listen to Status node output from the MQTT Out node
   if (msg.status && msg.status.text === "node-red:common.status.connected") {
       flow.set("is_online", true);
       // Trigger drain loop
       node.send({ topic: "TRIGGER_DRAIN" });
   } else if (msg.status && msg.status.text === "node-red:common.status.disconnected") {
       flow.set("is_online", false);
   }
   ```
2. **Buffer Function Node**:
   ```javascript
   const isOnline = flow.get("is_online") || false;
   if (isOnline) {
       return [msg, null]; // Output 1: Send to MQTT Out
   } else {
       let queue = flow.get("telemetry_queue") || [];
       if (queue.length < 5000) { // Safety ceiling to prevent V8 heap crash
           queue.push(msg.payload);
           flow.set("telemetry_queue", queue);
       }
       return [null, null];
   }
   ```
3. **Drain Loop**: A function node pops items from `flow.get("telemetry_queue")` and uses a `Delay` node (e.g., 50ms) to throttle outgoing transmission to avoid flooding the broker.

#### Pros & Cons
- **Pros**:
  - Zero external containers, zero external dependencies.
  - Sub-millisecond read/write speeds.
  - Extremely easy to prototype.
- **Cons**:
  - **Zero Crash Resilience**: If Node-RED restarts, crashes, or the machine loses power, **all buffered messages in RAM are permanently lost**.
  - **Memory Leak & OOM Risk**: An extended outage will fill the Node.js V8 heap, causing Node-RED to crash (`JavaScript heap out of memory`).
  - **Race Conditions**: Ensuring strict FIFO order during the transition from offline to online requires complex locking logic.

---

### Method B: File-Based Disk Spooling (JSON Lines / Append Log)

This method persists unsent messages to a local disk file using Node-RED's built-in **`file`** and **`file in`** nodes.

#### Architecture
1. When the MQTT node is disconnected, incoming payloads are serialized as JSON strings with a newline (`JSON.stringify(payload) + "\n"`).
2. The payload is piped to a **`file`** node configured to **append** to `/data/spool/offline_queue.jsonl`.
3. When the MQTT connection returns, a reader flow reads lines from the file, publishes them, and truncates the file once transmission is verified.

#### Pros & Cons
- **Pros**:
  - Persists data to disk without requiring a database engine.
  - Survives container and power reboots.
  - Built using standard native Node-RED nodes (`file`).
- **Cons**:
  - **High Implementation Complexity**: Managing file locks, file truncation, and atomic line deletions while new records are actively arriving is error-prone.
  - **Event Loop Blocking**: Synchronous disk I/O in Node.js can block the event loop, causing Modbus and OPC UA polling cycles to jitter or miss intervals.
  - **File Corruption Risk**: Sudden power loss during file truncation can corrupt the JSONL file.

---

### Method C: Embedded SQLite / LevelDB Queue (ACID-Compliant Queue)

This approach installs an embedded relational database (e.g., `node-red-node-sqlite`) inside the Node-RED container to act as a structured, transactional FIFO queue.

#### Architecture & Schema
```sql
CREATE TABLE IF NOT EXISTS edge_telemetry_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT DEFAULT 'PENDING',  -- 'PENDING', 'SENT'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_status_id ON edge_telemetry_queue (status, id);
```

#### Implementation Flow
1. **Enqueue**: During normal operation or network disconnects, every message is inserted into `edge_telemetry_queue` with `status = 'PENDING'`.
2. **Drain Worker**: A background loop executes:
   ```sql
   SELECT id, topic, payload FROM edge_telemetry_queue WHERE status = 'PENDING' ORDER BY id ASC LIMIT 25;
   ```
3. **Dispatch & Confirm**: Messages are published to MQTT. Upon receiving confirmation, the worker deletes processed IDs:
   ```sql
   DELETE FROM edge_telemetry_queue WHERE id IN (?);
   ```

#### Pros & Cons
- **Pros**:
  - **ACID Transactional Integrity**: Zero risk of file corruption; resilient to power failure and container crashes.
  - **Strict FIFO Ordering**: Handled naturally by SQLite's autoincrement primary key (`ORDER BY id ASC`).
  - **Observable State**: Queue size and historical records can be inspected via SQL queries.
- **Cons**:
  - Requires compiling native C dependencies (`sqlite3`) into the Node-RED container image.
  - Slower write throughput compared to RAM or Mosquitto's internal append buffer.
  - Significant flow complexity: requires SQL nodes, error-retry state machines, and periodic database maintenance (`VACUUM`).

---

### Method D: Community Contrib Nodes (`node-red-contrib-store-and-forward`)

Pre-packaged community nodes (such as `node-red-contrib-store-and-forward` or `node-red-contrib-msg-queue`) provide ready-made queueing nodes that wrap connection-gating and disk/memory buffering.

#### Pros & Cons
- **Pros**:
  - Replaces dozens of custom function nodes with a single drag-and-drop block.
  - Built-in queue size limits, rate-limited draining, and connection state detection.
- **Cons**:
  - Dependency on third-party community maintainers; risk of abandonment or incompatibility across major Node-RED versions.
  - Often lacks enterprise-grade disk persistence (many contrib nodes buffer only in RAM or use simple unindexed flat files).

---

## 3. Comprehensive Comparison Matrix

| Evaluation Criteria | Sidecar Local Broker (Current: Mosquitto) | Direct Node-RED: In-Memory Queue | Direct Node-RED: File Spooling (`file`) | Direct Node-RED: Embedded SQLite | Direct Node-RED: Contrib Nodes |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Persistence Across Reboots** | **Yes**: Native C-level disk persistence (`mosquitto.db`). Survives reboots. | **No**: Lost on container restart, OOM, or power failure. | **Yes**: Appended to disk file on mounted volume. | **Yes**: ACID-compliant transactional database file. | **Varies**: Most are RAM-only; few support file spooling. |
| **Process Isolation & Decoupling** | **High**: Poller and buffer are separate processes. If Node-RED restarts, buffer survives. | **None**: Tied to the Node-RED JavaScript V8 runtime. | **None**: Tied to Node-RED process. | **Medium**: Database file is decoupled, but engine runs inside Node-RED. | **None**: Runs inside Node-RED process. |
| **RAM Footprint** | **~10–15 MB RAM** (constant, does not grow with queue size). | **Unbounded**: Grows with queue size; risk of Node.js OOM kill. | **Low**: Appends directly to disk stream. | **Moderate**: ~25–50 MB RAM for SQLite cache and bindings. | **Moderate**: Depends on configured queue limits. |
| **Flow Implementation Effort** | **Zero Flow Logic**: Node-RED simply publishes QoS 1 to `localhost:1883`. | **Moderate**: Status node, switch, array push/pop, and drain timer. | **High**: File rotation, read/write locks, line parsing, truncation. | **High**: SQL schema migrations, transaction flows, worker loops. | **Low**: Single drag-and-drop node. |
| **Strict FIFO Ordering** | **Guaranteed**: Built into the MQTT 3.1.1 protocol specification. | **Difficult**: Race conditions during offline-to-online transitions. | **Moderate**: Sequential file reads. | **Guaranteed**: Handled by `ORDER BY id ASC`. | **Guaranteed**: Internal queue implementation. |
| **Drain Rate Regulation** | **Automatic**: Native MQTT windowing (`max_inflight_messages`). | **Manual**: Must implement manual `delay` / pacing flow. | **Manual**: Must pace file line emission to avoid broker overload. | **Controlled**: Can batch queries via `LIMIT N`. | **Configurable**: Built-in messages/sec setting. |
| **Max Tested Capacity** | **50,000+ messages** without degrading poller timing. | Limited to RAM (typically <5,000 msgs). | Millions (limited by disk space). | Millions (limited by disk space). | Typically <10,000 msgs. |

---

## 4. Architectural Recommendations

### When to use the Sidecar Local Broker (Current Architecture)
* **Production Manufacturing & Critical Infrastructure**: This is the industry-standard design mandated by **IEC 62443** and **ISA-95**. 
* **Benefits**:
  1. Complete decoupling of acquisition (Node-RED) from store-and-forward transport (Mosquitto).
  2. If Node-RED crashes or is restarted for flow redeployment, queued messages remain safe in the Mosquitto container.
  3. Uses almost no CPU and only ~12MB RAM.
  4. Requires zero custom JavaScript queueing code or community plugin dependencies.

### When to use Direct Node-RED Store-and-Forward
* **Bare-Metal Single-Process Constraints**: Running on legacy embedded hardware or microcontrollers where Docker is unavailable and running multiple system daemons is forbidden.
* **Ephemeral/Non-Critical Data**: Sensor readings where missing data during an infrequent power reboot is acceptable, and transient network dropouts (< 5 minutes) are the only concern (use **Method A: In-Memory**).
* **Self-Contained Edge Appliances**: When packaging a single container image that must not expose internal broker ports (use **Method C: Embedded SQLite**).

---

## 5. Live Demonstration: Testing the Current Store-and-Forward Engine

To observe the implemented Store-and-Forward mechanism in action:

1. **Monitor live ingestion**:
   ```bash
   docker compose logs -f --tail=10 mqtt-to-timescale
   ```
2. **Simulate a central plant outage by stopping the Central UNS broker**:
   ```bash
   docker compose stop mosquitto-uns
   ```
3. **Observe Edge autonomy**:
   - Check Node-RED at [http://localhost:1880](http://localhost:1880): pollers continue executing every 5s with green indicators.
   - Check Edge Mosquitto disk buffer:
     ```bash
     ls -lh mosquitto/data/mosquitto.db
     ```
4. **Restore Central UNS connectivity**:
   ```bash
   docker compose start mosquitto-uns
   ```
5. **Verify backlog drain**:
   - Within 2–5 seconds, the bridge reconnects, flushes all buffered messages, and TimescaleDB receives the entire backlog with original timestamps preserved.

---

## 6. QoS 1 vs. QoS 0: Is QoS 1 Strictly Necessary for Store-and-Forward?

A frequent architectural question in Industrial IoT is:
> *"Does Store-and-Forward require every message across the entire enterprise to use QoS 1? Is QoS 1 absolutely necessary everywhere?"*

**Short Answer**:
- **Over the WAN/Bridge link (Edge Broker $\rightarrow$ Central UNS)**: **YES, absolutely mandatory**. Without QoS $\ge 1$, MQTT brokers cannot hold messages in a persistent offline buffer because the eviction mechanism relies on the packet acknowledgment (`PUBACK`).
- **Between Node-RED and Local Edge Broker**: **NO, optional**. Node-RED can publish at QoS 0 locally; the edge broker can "upgrade" the QoS across the bridge.
- **Between Central UNS and Consumers**: **NO, depends on consumer intent**. Historians require QoS 1; live operator dashboards (HMIs/Grafana) actually prefer QoS 0 to avoid being flooded by historical data during reconnects.

---

### 6.1 The Protocol Constraint: Why Store-and-Forward Demands QoS $\ge 1$

Under the **OASIS MQTT 3.1.1 and 5.0 specifications**, the two QoS levels behave fundamentally differently during disconnections:

```
[ QoS 0: At most once / "Fire and Forget" ]
   Sender ───────────────── PUBLISH (mid: None) ─────────────────► Receiver (No ACK)
   * If network is down: Dropped immediately. No buffering, no retry.

[ QoS 1: At least once / "Acknowledged Delivery" ]
   Sender ───────────────── PUBLISH (mid: 1042) ─────────────────► Receiver
   Sender ◄──────────────── PUBACK  (mid: 1042) ───────────────── Receiver
   * If network is down: Kept in persistent queue until PUBACK is received.
```

1. **No Packet Identifier in QoS 0**:
   - QoS 0 packets do **not** carry a 16-bit Packet Identifier (`mid`).
   - Because there is no packet identifier, the receiver cannot acknowledge delivery (`PUBACK`), and the sender has no way to track whether a packet succeeded or failed.
2. **Buffer Eviction Mechanics**:
   - In Mosquitto (and every compliant MQTT broker), messages are stored in memory/disk and only evicted once the matching `PUBACK` returns from the remote peer.
   - If a bridge or client disconnects, the broker **immediately discards QoS 0 messages** directed to that offline session.
   - **Conclusion**: Store-and-Forward cannot exist over QoS 0 across a broken network link.

---

### 6.2 Link-by-Link Breakdown in the ISA-95 Architecture

To optimize performance without sacrificing data integrity, consider the three separate communication links in our topology:

```
Link 1: Node-RED ──(Localhost)──► Edge Mosquitto ──(WAN/DMZ Bridge)──► Central UNS ──► Downstream Consumers
                                     [Link 2]                                               [Link 3]
```

#### Link 1: Node-RED to Local Edge Mosquitto (`localhost:1883`)
* **Current Configuration**: QoS 1.
* **Is QoS 1 Strictly Necessary?**: **No**.
* **Analysis**:
  - Both containers run on the same physical edge host communicating over Docker's internal bridge or Unix domain sockets. Packet loss on `localhost` is virtually zero.
  - **QoS Upgrade Mechanism**: In Mosquitto, the bridge configuration directive:
    ```conf
    topic enterprise/# out 1
    ```
    The trailing `1` forces all outbound bridged messages to be transmitted at **QoS 1**, even if Node-RED originally published them locally at **QoS 0**!
  - **Trade-off**: Switching Node-RED to QoS 0 eliminates Node.js event-loop overhead waiting for local `PUBACK` packets, maximizing polling throughput. The only failure mode is if the edge host suffers sudden power loss before Mosquitto writes the incoming QoS 0 packet to `mosquitto.db`.

#### Link 2: Edge Mosquitto to Central Plant UNS (`edge-to-uns` Bridge)
* **Current Configuration**: QoS 1 (`topic enterprise/# out 1`).
* **Is QoS 1 Strictly Necessary?**: **Yes, 100% Mandatory**.
* **Analysis**:
  - This link traverses physical switches, plant VLAN boundaries, firewalls, or cellular/satellite WAN connections.
  - If configured with QoS 0 (`topic enterprise/# out 0`), Mosquitto will **not** queue messages when the bridge goes offline. Every single telemetry point generated during an outage would be discarded at the edge.
  - QoS 1 guarantees that messages stay in `mosquitto.db` on disk until the central broker acknowledges them with `PUBACK`.

#### Link 3: Central UNS to Downstream Consumers
* **Current Configuration**: Mixed (Historians subscribe at QoS 1; Web UIs subscribe at QoS 0 or 1).
* **Is QoS 1 Strictly Necessary?**: **No, it should be selective based on data consumer intent**:

| Consumer Category | Target System | Recommended QoS | Rationale |
| :--- | :--- | :--- | :--- |
| **Audit & Compliance** | TimescaleDB / DynamoDB / Lakehouse | **QoS 1** | **Mandatory**: Missing data points invalidate regulatory compliance, root-cause anomaly detection, and accurate OEE calculations. |
| **Real-Time HMI / SCADA** | Operator Gauges / Live WebSockets | **QoS 0** | **Recommended**: An operator looking at an instantaneous speed dial needs the *current* state. Replaying 50,000 stale historical points during reconnect causes gauge flickering, needle jumps, and browser freezing. |
| **High-Frequency Waveforms** | 10 kHz Vibration / Acoustic Sensors | **QoS 0** (or chunked QoS 1) | Transmitting thousands of individual QoS 1 packets/sec causes severe network ACK storm congestion. Often streamed via QoS 0 or aggregated into zipped Parquet batches sent via QoS 1. |
| **MES / Production Control** | Work Order Lifecycle / Batch Recipes | **QoS 1** (Retained) | Ensures state transitions (e.g., `START_ORDER`, `COMPLETE_ORDER`) are never lost. |

---

### 6.3 Technical Trade-offs: QoS 1 vs. QoS 0 in Production IIoT

| Dimension | QoS 0 (At most once) | QoS 1 (At least once) | Operational Impact |
| :--- | :--- | :--- | :--- |
| **Network Overhead** | 1 packet per message (`PUBLISH`). | 2 packets per message (`PUBLISH` + `PUBACK`). | QoS 1 doubles packet count and increases WAN bandwidth consumption. |
| **Message Deduplication** | Guaranteed zero duplicates. | Possible duplicates (`DUP = 1`). | If a network cut occurs after the broker receives `PUBLISH` but before `PUBACK` reaches the sender, the message is re-sent. **Downstream databases must use idempotent upserts** (e.g., `ON CONFLICT DO UPDATE`). |
| **Memory & Storage Pressure** | Constant (messages never queue). | Dynamically increases during outages. | Must set `max_queued_messages` and enforce disk persistence (`autosave_interval`). |
| **Edge Compute Impact** | Minimal CPU / event-loop cost. | Sender tracks packet IDs (`mid`) and in-flight windows. | Negligible for Mosquitto (compiled C); slight tick cost in JavaScript/Node-RED under heavy loads. |

---

### 6.4 Summary Best-Practice Recommendation

```
┌─────────────────┐  QoS 0 or 1  ┌─────────────────┐       QoS 1       ┌─────────────────┐
│ Node-RED Edge   ├─────────────►│ Edge Mosquitto  ├──────────────────►│ Central UNS     │
│ (Data Poller)   │  (Localhost) │ (Local Buffer)  │   (WAN/Bridge)    │ (Enterprise Hub)│
└─────────────────┘              └─────────────────┘                   └────────┬────────┘
                                                                                │
                                                 ┌──────────────────────────────┴──────────────────────────────┐
                                                 │ QoS 1 (Store & Forward)                                     │ QoS 0 (Real-Time Only)
                                                 ▼                                                             ▼
                                     ┌─────────────────────────┐                                   ┌─────────────────────────┐
                                     │ TimescaleDB / Cold Lake │                                   │ Live HMI / Grafana UI   │
                                     │ (Complete History)      │                                   │ (Instantaneous Needle)  │
                                     └─────────────────────────┘                                   └─────────────────────────┘
```

1. **Keep Link 2 at QoS 1**: It is the backbone of your Store-and-Forward guarantee.
2. **Ensure Downstream Idempotency**: Because QoS 1 guarantees *at least once* delivery, your consumers (such as `mqtt-to-timescale` or Kafka/Redpanda sinks) must deduplicate based on `(source_timestamp, topic)`.
3. **Segregate Real-Time from Historical Consumers**: Have live dashboards subscribe at QoS 0 to maintain smooth UI responsiveness during post-outage backlog flushes.

