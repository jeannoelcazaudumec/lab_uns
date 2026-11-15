# Transport & Deployment Guide: Fresh Machine Setup (Two-Tier Mosquitto UNS Edition)

This guide provides end-to-end instructions for exporting this complete Industrial IoT data pipeline and deploying it onto a fresh machine running Docker.

---

## 1. Overview & Portability Architecture

The complete 14-container pipeline—including field simulators (`modbus1`, `modbus2`, `opcua1`), Node-RED edge gateway with the `MES_Simul` and `Schema_Fault_Simul` flows, Mosquitto edge broker with native Mosquitto UNS bridging, Mosquitto Central UNS (`eclipse-mosquitto:latest`) UNS broker, `uns-enricher` JSON Schema validator and payload augmenter, `mqtt-to-timescale` sink + Mosquitto UNS Inspector, TimescaleDB hypertables (`asset_telemetry`, `asset_dlq`, `mosquitto-uns_*`), `connect-uns-to-dynamodb` (Redpanda Connect), DynamoDB Local (`dynamodb-local` + `dynamodb-init`), `nodered-asset3` IT consumer, and Grafana dashboards—is defined as code:

- **Self-Contained Configuration**: All custom images (`nodered`, `nodered-asset3`, `modbus1`, `modbus2`, `opcua1`, `uns-enricher`, `mqtt-to-timescale`, and `grafana`) have local Dockerfiles.
- **Automated Provisioning**: TimescaleDB initializes the `asset_telemetry`, `asset_dlq`, and `mosquitto-uns_*` inspector tables automatically on startup via `init.sql`; `dynamodb-init` automatically provisions the `asset_telemetry` table in DynamoDB Local; Grafana automatically provisions both datasources (`TimescaleDB`, `Mosquitto UNS` with pre-baked `grafana-mqtt-datasource`) and all 3 dashboards (`mosquitto-uns_uns_inspector.json`, `mosquitto-uns_telemetry.json`, `modbus_telemetry.json`) via YAML configs.

---

## 2. Creating the Portable Tar Archive

From the parent directory containing the repository, create a clean, lightweight archive that excludes runtime database files, caches, and logs:

```bash
cd ..

tar -czvf nodered-mosquitto-uns-clean.tar.gz \
  --exclude='*/timescaledb/data/*' \
  --exclude='*/dynamodb/data/*' \
  --exclude='*/mosquitto-uns/data/*' \
  --exclude='*/mosquitto-uns/log/*' \
  --exclude='*/mosquitto/data/*' \
  --exclude='*/mosquitto/log/*' \
  --exclude='*/nodered/data/*.backup' \
  --exclude='*/nodered-app/data/*.backup' \
  --exclude='*/__pycache__' \
  --exclude='*.pyc' \
  --exclude='*/grafana/plugins' \
  nodered-mosquitto/
```

---

## 3. Network & Host Port Availability

Ensure the following host ports are available on `127.0.0.1` on the target machine:
- `127.0.0.1:502`, `127.0.0.1:503`: Modbus TCP Simulators (`modbus1`, `modbus2`)
- `127.0.0.1:4840`: OPC UA Simulator (`opcua1`)
- `127.0.0.1:1880`: Node-RED Edge Gateway Web UI (includes `MES_Simul` and `Schema_Fault_Simul` flow tabs)
- `127.0.0.1:1881`: Node-RED Asset 3 IT Consumer Web UI
- `127.0.0.1:1883`: Edge Mosquitto MQTT Broker
- `127.0.0.1:1884`: Mosquitto Central UNS MQTT Broker (mapped to container port 1883)
- `127.0.0.1:8083`: Mosquitto Central UNS MQTT over WebSockets (mapped to container port 8000 at `/mqtt`)
- `127.0.0.1:3001`: Grafana Dashboards & Mosquitto Central UNS Control Center (`admin` / `admin`, mapped to container port 3000)
- `127.0.0.1:5432`: TimescaleDB (`postgres` / `postgres`)
- `127.0.0.1:8000`: DynamoDB Local (`asset_telemetry` table)

*(Note: All published ports are strictly bound to `127.0.0.1` for host isolation. Containers run under least-privilege non-root `appuser: 10001` with explicit CPU/memory limits and native healthchecks.)*

---

## 4. Restore, Deploy & Launch on the Target Machine

```bash
tar -xzvf nodered-mosquitto-uns-clean.tar.gz
cd nodered-mosquitto
mkdir -p timescaledb/data dynamodb/data mosquitto-uns/data mosquitto-uns/log mosquitto/data mosquitto/log
chmod -R 777 mosquitto-uns/data mosquitto-uns/log mosquitto/data mosquitto/log dynamodb/data
docker compose up -d --build
```

### Verification & Smoke Testing

1. **Verify All 14 Services**:
   ```bash
   docker compose ps -a
   ```
2. **Verify Schema-Validated & Augmented Telemetry (with `part_number`) in TimescaleDB**:
   ```bash
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT time, payload->>'asset_id' AS asset, payload->>'part_number' AS part_number, payload->>'schema_valid' AS valid, payload->>'temperature' AS temp, payload->>'humidity' AS hum, payload->>'motor_speed' AS rpm 
   FROM asset_telemetry 
   ORDER BY time DESC 
   LIMIT 6;
   "
   ```
3. **Verify Rejected Schema Violations in TimescaleDB (`asset_dlq`)**:
   ```bash
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT time, asset_id, error_type, failed_field, payload->>'schema_rule' AS rule, reason
   FROM asset_dlq
   ORDER BY time DESC
   LIMIT 5;
   "
   ```
4. **Verify Augmented Items in DynamoDB Local**:
   ```bash
   python3 dynamodb/query_table.py
   ```
5. **Verify Connected Mosquitto UNS Sessions, Subscriptions & Topics**:
   ```bash
   docker compose exec timescaledb psql -U postgres -d telemetry \
     -c "SELECT client_id, status, role, ip_address, clean_start, messages_in, messages_out FROM uns_clients;" \
     -c "SELECT subscription_id, client_id, client_status, matched_topics_count, routed_messages FROM uns_subscriptions;" \
     -c "SELECT topic, stage, publisher_client_id, retained, message_count FROM uns_topics ORDER BY stage, topic;"
   ```

---

## 5. Running on WSL2 (Windows Subsystem for Linux)

The lab is **100% portable and fully supported on WSL2**. When deploying onto a machine running WSL2, observe the following best practices:

### A. Extract Inside the Linux Filesystem (Crucial)
Extract and run the repository inside the WSL2 ext4 filesystem (e.g., `~/nodered-mosquitto` or `/home/<user>/...`), **not** on a Windows-mounted drive like `/mnt/c/...`:
- **Why**: PostgreSQL (`timescaledb`) strictly enforces Linux directory permissions (`0700`) and POSIX file locking. The Windows 9P/DrvFs translation layer on `/mnt/c/` breaks POSIX permissions and causes severe disk I/O latency. Running in `~/` guarantees native Linux performance and permissions.

### B. Accessing Web UIs from Windows
WSL2 features automatic **localhost forwarding** by default (`localhostForwarding=true`):
- All published container endpoints bound to `127.0.0.1` inside WSL2 are immediately accessible from your Windows host browsers (Chrome, Edge, Firefox) at:
  - Node-RED Edge: [http://localhost:1880](http://localhost:1880)
  - Grafana Dashboards: [http://localhost:3001](http://localhost:3001)
  - Node-RED Asset 3 IT App: [http://localhost:1881](http://localhost:1881)
  - DynamoDB Local: [http://localhost:8000](http://localhost:8000)
  - Central UNS WebSocket: `ws://localhost:8083`

### C. Docker Engine Options in WSL2
Both standard WSL2 Docker setups are fully compatible:
1. **Docker Desktop for Windows**: Enable *"Use the WSL 2 based engine"* and turn on WSL Integration for your distro in Docker Desktop Settings -> Resources -> WSL Integration.
2. **Native Docker Engine (Docker CE in WSL2)**: Installed directly inside Ubuntu/Debian (`sudo apt-get install docker.io docker-compose-plugin`). Ensure dockerd is running via `sudo service docker start` or WSL2 `systemd`.

### D. Port 502 (Modbus TCP) Host Conflicts
If your Windows host is already running a local Modbus service or Siemens/Rockwell PLC emulator occupying host port 502, override the external port without modifying `docker-compose.yml`:
```bash
export MODBUS1_HOST_PORT=1502
export MODBUS2_HOST_PORT=1503
docker compose up -d
```

### E. Kernel Sysctls & Non-Root Execution
The lab's unprivileged user configuration (`appuser: 10001`) and namespace sysctl:
```yaml
sysctls:
  net.ipv4.ip_unprivileged_port_start: 0
```
are natively supported by WSL2 (which runs Linux kernel 5.15+ or 6.x, well above the 4.19 requirement).
