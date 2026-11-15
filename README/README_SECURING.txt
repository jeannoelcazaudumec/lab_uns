===============================================================================
TWO-TIER MOSQUITTO UNS SECURITY & HARDENING GUIDE (PURDUE / IEC 62443 DMZ)
===============================================================================

To properly secure connections between the OT Edge, the Central UNS Broker
(`mosquitto-uns`), and enterprise IT systems in an Industrial IoT (Purdue Zone 3.5
IT DMZ) architecture, implement four layers of defense:
  1. Transport Encryption (TLS / mTLS on Port 8883 & Secure WebSockets on 8083)
  2. Authentication (Disabling `allow_anonymous` + Adding Password File Authentication)
  3. Topic-Level Authorization (Least-Privilege Mosquitto ACLs)
  4. Network & Host Port Hardening (Restricting Bind Interfaces & Rate Limits)

TARGET SECURE TOPOLOGY:
  [edge_ot_it]                       [it_dmz]                           [it]
  mosquitto ─── TLS 8883 + Auth ──> mosquitto-uns (:8883) <── TLS 8883 + Auth ── mqtt-to-timescale
  (Write-only: .../raw/ & .../mes/state/)    ▲           <── TLS 8883 + Auth ── connect-uns-to-dynamodb
                                             │           <── TLS 8883 + Auth ── nodered-asset3 (Read-only: asset_3)
                                             ▼           <── TLS 8883 + Auth ── grafana (Read-only: enterprise/#)
                                        uns-enricher
                                   (Sub: .../raw/ & .../mes/state/,
                                    Pub: .../telemetry/ & .../dlq/)


===============================================================================
LAYER 1: TRANSPORT ENCRYPTION (TLS / mTLS ON PORT 8883)
===============================================================================

Replace plaintext MQTT (1883) with MQTT over TLS (8883) on `mosquitto-uns` so
credentials and industrial telemetry payloads cannot be sniffed or tampered with
in transit.

1. Generate an Internal PKI (CA, Server Certs, and Client Certs):
   Create a local Certificate Authority (`ca.crt`, `ca.key`), and a broker
   server certificate signed with Subject Alternative Name (SAN)
   `DNS:mosquitto-uns,DNS:localhost,IP:127.0.0.1`:

   ```bash
   mkdir -p certs
   # Root CA
   openssl genrsa -out certs/ca.key 4096
   openssl req -x509 -new -nodes -key certs/ca.key -sha256 -days 3650 \
     -subj "/CN=IIoT-Root-CA/O=Industrial-Enterprise" -out certs/ca.crt

   # Mosquitto UNS Server Certificate
   openssl genrsa -out certs/server.key 2048
   openssl req -new -key certs/server.key -subj "/CN=mosquitto-uns" -out certs/server.csr
   printf "subjectAltName=DNS:mosquitto-uns,DNS:localhost,IP:127.0.0.1" > certs/server_ext.cnf
   openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key \
     -CAcreateserial -out certs/server.crt -days 825 -sha256 -extfile certs/server_ext.cnf
   ```

2. Configure TLS Listeners in `mosquitto-uns/config/mosquitto.conf`:
   ```text
   # Standard TLS Listener
   listener 8883
   cafile /mosquitto/config/certs/ca.crt
   certfile /mosquitto/config/certs/server.crt
   keyfile /mosquitto/config/certs/server.key
   tls_version tlsv1.3

   # Secure WebSocket Listener
   listener 8083
   protocol websockets
   cafile /mosquitto/config/certs/ca.crt
   certfile /mosquitto/config/certs/server.crt
   keyfile /mosquitto/config/certs/server.key
   ```

3. Update Upstream & Downstream Clients for TLS (Port 8883):
   A) OT Edge Mosquitto Bridge (`mosquitto/config/mosquitto.conf`):
      ```text
      connection edge-to-uns
      address mosquitto-uns:8883
      remote_clientid edge-to-uns
      topic enterprise/# out 1
      cleansession false
      bridge_cafile /mosquitto/config/certs/ca.crt
      bridge_insecure false
      ```

   B) UNS Context Enricher (`connect/uns-enricher/enricher.py`) &
      TimescaleDB Sink (`connect/mqtt-to-timescale/sink.py`):
      ```python
      client.tls_set(ca_certs="/app/certs/ca.crt")
      client.connect(MQTT_HOST, 8883, keepalive=30)
      ```

   C) Redpanda Connect -> DynamoDB (`connect/mosquitto-to-dynamodb.yaml`):
      ```yaml
      input:
        mqtt:
          urls: ["ssl://mosquitto-uns:8883"]
          tls:
            enabled: true
            root_cas_file: /etc/certs/ca.crt
      ```

   D) Node-RED Asset 3 Consumer (`nodered-app/data/flows.json`):
      Enable "Use TLS" (`usetls: true`) on the `mosquitto_broker_uns` config node,
      set port to `8883`, and attach a TLS configuration pointing to `/data/certs/ca.crt`.

   E) Grafana MQTT Datasource (`grafana/provisioning/datasources/datasource.yaml`):
      ```yaml
      jsonData:
        uri: "tcps://mosquitto-uns:8883"
        tlsAuthWithCACert: true
      secureJsonData:
        tlsCACert: |
          -----BEGIN CERTIFICATE-----
          ...
          -----END CERTIFICATE-----
      ```


===============================================================================
LAYER 2: AUTHENTICATION (PASSWORD FILE HARDENING)
===============================================================================

Disable anonymous access in `mosquitto-uns/config/mosquitto.conf`:
```text
allow_anonymous false
password_file /mosquitto/config/passwd
```

Create user accounts with salted SHA-512 passwords using `mosquitto_passwd`:
```bash
docker compose exec mosquitto-uns mosquitto_passwd -c /mosquitto/config/passwd edge_bridge
docker compose exec mosquitto-uns mosquitto_passwd -b /mosquitto/config/passwd uns_enricher SecurePassword123!
docker compose exec mosquitto-uns mosquitto_passwd -b /mosquitto/config/passwd timescale_sink SecurePassword123!
docker compose exec mosquitto-uns mosquitto_passwd -b /mosquitto/config/passwd dynamodb_sink SecurePassword123!
docker compose exec mosquitto-uns mosquitto_passwd -b /mosquitto/config/passwd nodered_asset3 SecurePassword123!
docker compose exec mosquitto-uns mosquitto_passwd -b /mosquitto/config/passwd grafana_reader SecurePassword123!
```


===============================================================================
LAYER 3: TOPIC-LEVEL AUTHORIZATION (LEAST-PRIVILEGE MOSQUITTO ACLs)
===============================================================================

Enable ACL checking in `mosquitto-uns/config/mosquitto.conf`:
```text
acl_file /mosquitto/config/acl.conf
```

Create `/mosquitto/config/acl.conf` with least-privilege topic rules:
```text
# 1. Edge Bridge: Can publish raw OT data and MES context only
user edge_bridge
topic write enterprise/plant/area/sub-area/cell/+/raw/
topic write enterprise/plant/area/sub-area/cell/mes/state/

# 2. UNS Enricher: Subscribes to raw OT & MES, publishes verified telemetry & DLQ
user uns_enricher
topic read enterprise/plant/area/sub-area/cell/+/raw/
topic read enterprise/plant/area/sub-area/cell/mes/state/
topic write enterprise/plant/area/sub-area/cell/+/telemetry/
topic write enterprise/plant/area/sub-area/cell/+/dlq/

# 3. TimescaleDB Sink: Consumes validated telemetry and DLQ records
user timescale_sink
topic read enterprise/plant/area/sub-area/cell/+/telemetry/
topic read enterprise/plant/area/sub-area/cell/+/dlq/

# 4. DynamoDB Sink: Consumes validated telemetry
user dynamodb_sink
topic read enterprise/plant/area/sub-area/cell/+/telemetry/

# 5. Asset 3 Downstream App: Consumes Asset 3 telemetry only
user nodered_asset3
topic read enterprise/plant/area/sub-area/cell/asset_3/telemetry/

# 6. Grafana Dashboard Reader: Read-only access for observability
user grafana_reader
topic read enterprise/#
```


===============================================================================
LAYER 4: NETWORK, CONTAINER & RUNTIME HARDENING (IMPLEMENTED)
===============================================================================

1. Restrict Host Port Exposure to Loopback (`127.0.0.1`):
   All published host ports in `docker-compose.yml` are strictly bound to `127.0.0.1`,
   guaranteeing that services are inaccessible from external physical networks or
   untrusted WAN interfaces:
   - Modbus & OPC UA: `127.0.0.1:502:502`, `127.0.0.1:503:502`, `127.0.0.1:4840:4840`
   - Edge & Central MQTT: `127.0.0.1:1883:1883`, `127.0.0.1:1884:1883`, `127.0.0.1:8083:8000`
   - Node-RED Gateways: `127.0.0.1:1880:1880`, `127.0.0.1:1881:1880`
   - TimescaleDB & DynamoDB: `127.0.0.1:5432:5432`, `127.0.0.1:8000:8000`
   - Grafana Observability: `127.0.0.1:3001:3000`

2. Least-Privilege Non-Root Execution (`appuser: 10001`):
   All custom Python containers (`opcua1`, `uns-enricher`, `mqtt-to-timescale`,
   `modbus1`, `modbus2`) build with a dedicated, unprivileged user:
   ```dockerfile
   RUN groupadd -g 10001 appuser && useradd -u 10001 -g appuser -s /bin/sh -d /home/appuser appuser
   USER 10001:10001
   ```

3. Unprivileged Low-Port Binding for Modbus TCP:
   To run Modbus TCP servers on standard port 502 as non-root `appuser` without
   granting full `CAP_NET_BIND_SERVICE`, `docker-compose.yml` configures:
   ```yaml
   sysctls:
     net.ipv4.ip_unprivileged_port_start: 0
   ```

4. Deterministic Container Healthchecks & Dependency Ordering:
   `mosquitto`, `mosquitto-uns`, and `timescaledb` run native healthchecks.
   Downstream sinks (`mqtt-to-timescale`, `uns-enricher`, `connect-uns-to-dynamodb`)
   declare `condition: service_healthy` to prevent startup connection crashes.

5. Container Resource Quotas (DoS & Leak Prevention):
   Explicit CPU and Memory limits are defined for all containers in `docker-compose.yml`
   (e.g., `timescaledb`: 1.0 CPU, 512MB RAM; `mosquitto-uns`: 0.50 CPU, 256MB RAM;
   `uns-enricher`: 0.50 CPU, 256MB RAM; field devices: 0.25 CPU, 128MB RAM).

6. Connection & Memory Safeguards in `mosquitto.conf`:
   Add connection restrictions to prevent denial-of-service (DoS) or memory exhaustion:
   ```text
   max_connections 500
   max_queued_messages 50000
   max_packet_size 10485760
   message_size_limit 20971520
   persistent_client_expiration 7d
   ```
===============================================================================
