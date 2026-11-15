===============================================================================
TIMESCALE DB MONITORING & OPERATIONS GUIDE (TWO-TIER MOSQUITTO UNS EDITION)
===============================================================================

1. CONNECTION DETAILS & SCHEMA
-------------------------------------------------------------------------------
- Container Name : timescaledb
- Image          : timescale/timescaledb:latest-pg15
- Network Zone   : it
- Host / Port    : localhost:5432 (or <machine-ip>:5432)
- Database       : telemetry
- Username       : postgres
- Password       : postgres
- Hypertables    : public.asset_telemetry      (Validated & Augmented Telemetry)
                   public.asset_dlq            (Quarantined Schema Violations / DLQ)
                   public.uns_topic_metrics (5s UNS Topic Throughput & Bandwidth)
                   public.uns_client_events (Mosquitto UNS Native event.log Audit Trail)
- State Tables   : public.uns_clients       (Live Connected & Persistent MQTT Clients)
                   public.uns_subscriptions (Active MQTT Subscriptions & Routed Counts)
                   public.uns_topics        (Active UNS Topics & Latest JSON Payloads)

Table Definitions (timescaledb/init.sql):
  CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

  CREATE TABLE IF NOT EXISTS asset_telemetry (
      time TIMESTAMPTZ NOT NULL,
      payload JSONB NOT NULL
  );
  SELECT create_hypertable('asset_telemetry', 'time', if_not_exists => TRUE);
  CREATE INDEX IF NOT EXISTS idx_asset_telemetry_payload
      ON asset_telemetry USING gin (payload);

  CREATE TABLE IF NOT EXISTS asset_dlq (
      time TIMESTAMPTZ NOT NULL,
      asset_id TEXT,
      error_type TEXT,
      failed_field TEXT,
      reason TEXT,
      payload JSONB NOT NULL
  );
  SELECT create_hypertable('asset_dlq', 'time', if_not_exists => TRUE);
  CREATE INDEX IF NOT EXISTS idx_asset_dlq_payload
      ON asset_dlq USING gin (payload);

  -- Plus Mosquitto Central UNS Broker Inspector tables:
  -- uns_clients, uns_subscriptions, uns_topics,
  -- uns_topic_metrics (hypertable), and uns_client_events (hypertable).

  -- Automated Columnar Compression & Retention Policies (timescaledb/init.sql):
  ALTER TABLE asset_telemetry SET (
      timescaledb.compress,
      timescaledb.compress_orderby = 'time DESC'
  );
  SELECT add_compression_policy('asset_telemetry', INTERVAL '2 days', if_not_exists => TRUE);
  SELECT add_retention_policy('asset_telemetry', INTERVAL '90 days', if_not_exists => TRUE);
  SELECT add_retention_policy('uns_topic_metrics', INTERVAL '14 days', if_not_exists => TRUE);
  SELECT add_retention_policy('uns_client_events', INTERVAL '14 days', if_not_exists => TRUE);

Note on Schema Validation, Schema-on-Read, Compression & UNS Augmentation:
  1. `uns-enricher` validates all raw payloads against `telemetry_schema.json`
     before augmenting them with `part_number`, `batch_id`, and `schema_valid: true`.
  2. Validated payloads are written to `asset_telemetry`, where the GIN-indexed
     `JSONB` column indexes `part_number` and `batch_id` automatically.
  3. Non-compliant payloads are blocked from `asset_telemetry` and routed via
     `enterprise/.../dlq/` into the `asset_dlq` quarantine hypertable.
  4. TimescaleDB automatically compresses chunks older than 2 days using columnar
     compression ordered by `time DESC`, slashing disk usage by ~90%.
  5. Automated retention policies automatically purge raw telemetry older than 90 days
     and topic/client audit metrics older than 14 days.
  6. `UNS Inspector` inside `mqtt-to-timescale` (running as unprivileged `appuser: 10001`)
     tails `mosquitto-uns/log/mosquitto.log` and monitors `enterprise/#` to maintain
     real-time Mosquitto UNS client, subscription, and topic tables in TimescaleDB.


2. BROWSER MONITORING VIA GRAFANA (:3001)
-------------------------------------------------------------------------------
- Login Credentials: admin / admin

A) Mosquitto Central UNS Control Center - Topics, Subscriptions & Clients:
   http://localhost:3001/d/mosquitto-uns-control-center/mosquitto-uns-control-center
   Provides an EMQX-Dashboard-equivalent control plane for Mosquitto UNS:
   broker status, connected clients, persistent sessions, active subscriptions,
   wildcard topic routing, ISA-95 topic tree with latest JSON payload previews,
   and the native `mosquitto-uns/log/mosquitto.log` client connection audit log.

B) Mosquitto Central UNS Streaming Telemetry - Direct MQTT Stream:
   http://localhost:3001/d/mosquitto-telemetry/mosquitto-telemetry
   Streams live MQTT messages directly from `tcp://mosquitto-uns:1883` (`.../+/telemetry/`
   and `.../+/dlq/`).

C) Pre-Provisioned Historical SQL Dashboard:
   http://localhost:3001/d/modbus-telemetry/plant-telemetry-modbus-and-opc-ua-sensors
   Displays live stat cards for Sensor 1 (modbus1), Sensor 2 (modbus2), and
   Sensor 3 (opcua1) Temperature, Humidity, and Motor Speed gauge, plus 1-hour
   historical trend curves queried directly from TimescaleDB.

D) Ad-Hoc SQL Query Explorer:
   http://localhost:3001/explore
   1. Select "TimescaleDB" from the top-left datasource dropdown.
   2. Switch the query editor from "Builder" to "Code".
   3. Run any PostgreSQL / TimescaleDB query directly in the browser.


3. LIVE TERMINAL MONITORING (PSQL CLI)
-------------------------------------------------------------------------------
A) Watch the newest incoming enriched records (with Part Number) every 2s:
   watch -n 2 'docker compose exec -T timescaledb psql -U postgres -d telemetry -c "
   SELECT time AT TIME ZONE '\''America/New_York'\'' AS time_edt,
          payload->>'\''asset_id'\'' AS asset,
          payload->>'\''part_number'\'' AS part_number,
          payload->>'\''batch_id'\'' AS batch_id,
          payload->>'\''temperature'\'' AS temp_c,
          payload->>'\''humidity'\'' AS hum_pct,
          payload->>'\''motor_speed'\'' AS rpm
   FROM asset_telemetry
   ORDER BY time DESC
   LIMIT 6;"'

B) Inspect Mosquitto UNS Connected Clients, Subscriptions & Topics:
   docker compose exec timescaledb psql -U postgres -d telemetry \
     -c "SELECT client_id, status, role, ip_address, clean_start, messages_in, messages_out FROM uns_clients ORDER BY clean_start, client_id;" \
     -c "SELECT subscription_id, client_id, client_status, topic_filter, matched_topics_count, routed_messages FROM uns_subscriptions ORDER BY subscription_id;" \
     -c "SELECT topic, stage, publisher_client_id, retained, subscriber_count, message_count, total_bytes FROM uns_topics ORDER BY stage, topic;"

C) Aggregate Telemetry Metrics Grouped by Part Number & Asset:
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT payload->>'part_number' AS part_number,
          payload->>'batch_id' AS batch_id,
          payload->>'asset_id' AS asset,
          ROUND(AVG((payload->>'temperature')::numeric), 2) AS avg_temp_c,
          ROUND(AVG((payload->>'humidity')::numeric), 2) AS avg_hum_pct,
          ROUND(AVG((payload->>'motor_speed')::numeric), 1) AS avg_rpm,
          COUNT(*) AS samples
   FROM asset_telemetry
   WHERE payload ? 'part_number'
   GROUP BY 1, 2, 3
   ORDER BY 1, 3;"

D) View latest raw JSONB payloads:
   docker compose exec timescaledb psql -U postgres -d telemetry -P pager=off -c "
   SELECT time, payload
   FROM asset_telemetry
   ORDER BY time DESC
   LIMIT 6;"

E) Inspect Quarantined Schema Violations in the Dead-Letter Queue (`asset_dlq`):
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

F) Check total record counts and latest timestamp per asset:
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT payload->>'asset_id' AS asset,
          COUNT(*) AS total_rows,
          MIN(time AT TIME ZONE 'America/New_York') AS first_seen_edt,
          MAX(time AT TIME ZONE 'America/New_York') AS last_seen_edt
   FROM asset_telemetry
   GROUP BY 1
   ORDER BY 1;"

G) Inspect Hypertable storage size and chunks:
   docker compose exec timescaledb psql -U postgres -d telemetry -c "
   SELECT hypertable_name,
          num_chunks,
          pg_size_pretty(total_bytes) AS total_size,
          pg_size_pretty(index_bytes) AS index_size,
          pg_size_pretty(table_bytes) AS table_size
   FROM timescaledb_information.hypertables
   JOIN hypertable_detailed_size('asset_telemetry') ON true
   WHERE hypertable_name = 'asset_telemetry';"

H) Inspect Automated Compression Policies & Background Jobs:
   docker compose exec timescaledb psql -U postgres -d telemetry \
     -c "SELECT hypertable_name, attname, orderby_column_index FROM timescaledb_information.compression_settings;" \
     -c "SELECT job_id, application_name, schedule_interval, max_runtime FROM timescaledb_information.jobs;"

I) Open an interactive psql shell:
   docker compose exec -it timescaledb psql -U postgres -d telemetry


4. MONITOR THE INGESTION SINK CONTAINER (MQTT-TO-TIMESCALE)
-------------------------------------------------------------------------------
Watch live QoS 1 messages consumed from Mosquitto UNS and inserted into TimescaleDB:
  docker compose logs -f --tail=20 mqtt-to-timescale


5. EXTERNAL GUI DATABASE CLIENTS (DBeaver, pgAdmin, DataGrip, VS Code)
-------------------------------------------------------------------------------
Connect using standard PostgreSQL driver settings:
  Host     : localhost (or host IP address)
  Port     : 5432
  Database : telemetry
  Username : postgres
  Password : postgres
  SSL Mode : disable
===============================================================================
