CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS asset_telemetry (
    time TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

SELECT create_hypertable('asset_telemetry', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_asset_telemetry_payload ON asset_telemetry USING gin (payload);

CREATE TABLE IF NOT EXISTS asset_dlq (
    time TIMESTAMPTZ NOT NULL,
    asset_id TEXT,
    error_type TEXT,
    failed_field TEXT,
    reason TEXT,
    payload JSONB NOT NULL
);

SELECT create_hypertable('asset_dlq', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_asset_dlq_payload ON asset_dlq USING gin (payload);

-- Mosquitto Central UNS Broker Inspector Tables (Clients, Subscriptions, Topics & Metrics)
CREATE TABLE IF NOT EXISTS uns_clients (
    client_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'CONNECTED',
    role TEXT NOT NULL,
    network_zone TEXT NOT NULL,
    ip_address TEXT NOT NULL DEFAULT '172.20.0.x',
    mqtt_version TEXT NOT NULL DEFAULT 'MQTT v3.1.1',
    clean_start BOOLEAN NOT NULL DEFAULT FALSE,
    session_expiry_sec BIGINT NOT NULL DEFAULT 4294967295,
    max_queue_size INTEGER NOT NULL DEFAULT 50000,
    subscriptions_count INTEGER NOT NULL DEFAULT 0,
    published_topics TEXT NOT NULL DEFAULT '-',
    messages_in BIGINT NOT NULL DEFAULT 0,
    messages_out BIGINT NOT NULL DEFAULT 0,
    connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS uns_subscriptions (
    subscription_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    client_role TEXT NOT NULL,
    client_status TEXT NOT NULL DEFAULT 'CONNECTED',
    topic_filter TEXT NOT NULL,
    qos INTEGER NOT NULL DEFAULT 1,
    clean_session BOOLEAN NOT NULL DEFAULT FALSE,
    persistence_mode TEXT NOT NULL DEFAULT 'Persistent Queue (50k msgs)',
    matched_topics_count INTEGER NOT NULL DEFAULT 0,
    routed_messages BIGINT NOT NULL DEFAULT 0,
    routed_bytes BIGINT NOT NULL DEFAULT 0,
    subscribed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_delivery TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS uns_topics (
    topic TEXT PRIMARY KEY,
    stage TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    publisher_client_id TEXT NOT NULL,
    qos INTEGER NOT NULL DEFAULT 1,
    retained BOOLEAN NOT NULL DEFAULT FALSE,
    subscriber_count INTEGER NOT NULL DEFAULT 0,
    subscribers TEXT NOT NULL DEFAULT '',
    message_count BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    last_payload_bytes INTEGER NOT NULL DEFAULT 0,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS uns_topic_metrics (
    time TIMESTAMPTZ NOT NULL,
    topic TEXT NOT NULL,
    stage TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    messages_delta BIGINT NOT NULL,
    bytes_delta BIGINT NOT NULL,
    total_messages BIGINT NOT NULL
);

SELECT create_hypertable('uns_topic_metrics', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS uns_client_events (
    time TIMESTAMPTZ NOT NULL,
    client_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    ip_address TEXT NOT NULL DEFAULT '-',
    clean_start TEXT NOT NULL DEFAULT '-',
    session_expiry TEXT NOT NULL DEFAULT '-',
    details TEXT NOT NULL
);

SELECT create_hypertable('uns_client_events', 'time', if_not_exists => TRUE);

-- Automated Columnar Compression & Retention Policies
ALTER TABLE asset_telemetry SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('asset_telemetry', INTERVAL '2 days', if_not_exists => TRUE);
SELECT add_retention_policy('asset_telemetry', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('uns_topic_metrics', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_retention_policy('uns_client_events', INTERVAL '14 days', if_not_exists => TRUE);
