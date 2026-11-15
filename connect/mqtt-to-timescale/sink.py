import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("mqtt_to_timescale")

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-uns")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv(
    "MQTT_TOPIC", "enterprise/plant/area/sub-area/cell/+/telemetry/"
)
DLQ_TOPIC = os.getenv(
    "DLQ_TOPIC", "enterprise/plant/area/sub-area/cell/+/dlq/"
)
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "timescaledb-group")
BROKER_LOG = os.getenv("BROKER_LOG", "/broker-log/mosquitto.log")

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "telemetry")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS asset_telemetry (
    time TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);
SELECT create_hypertable('asset_telemetry', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS asset_dlq (
    time TIMESTAMPTZ NOT NULL,
    asset_id TEXT,
    error_type TEXT,
    failed_field TEXT,
    reason TEXT,
    payload JSONB NOT NULL
);
SELECT create_hypertable('asset_dlq', 'time', if_not_exists => TRUE);

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
"""

KNOWN_CLIENT_PROFILES = {
    "edge-to-uns": {
        "role": "OT Edge Mosquitto Bridge (Producer)",
        "network_zone": "Level 2/3 -> Level 3.5 (edge_ot_it / it_dmz)",
        "mqtt_version": "MQTT v3.1.1 (Bridge)",
        "clean_start": False,
        "session_expiry_sec": 4294967295,
        "max_queue_size": 50000,
        "subscriptions_count": 0,
        "published_topics": "enterprise/plant/area/sub-area/cell/+/raw/, .../mes/state/",
    },
    "edge-to-uns": {
        "role": "OT Edge Mosquitto Bridge (Producer)",
        "network_zone": "Level 2/3 -> Level 3.5 (edge_ot_it / it_dmz)",
        "mqtt_version": "MQTT v3.1.1 (Bridge)",
        "clean_start": False,
        "session_expiry_sec": 4294967295,
        "max_queue_size": 50000,
        "subscriptions_count": 0,
        "published_topics": "enterprise/plant/area/sub-area/cell/+/raw/, .../mes/state/",
    },
    "uns-enricher": {
        "role": "In-UNS Schema Gatekeeper & MES Enricher",
        "network_zone": "Level 3.5 DMZ (it_dmz)",
        "mqtt_version": "MQTT v3.1.1",
        "clean_start": False,
        "session_expiry_sec": 4294967295,
        "max_queue_size": 50000,
        "subscriptions_count": 2,
        "published_topics": "enterprise/plant/area/sub-area/cell/+/telemetry/, .../+/dlq/",
    },
    "timescaledb-group": {
        "role": "TimescaleDB Transactional Historian Sink",
        "network_zone": "Level 4 IT (it_dmz / it)",
        "mqtt_version": "MQTT v3.1.1",
        "clean_start": False,
        "session_expiry_sec": 4294967295,
        "max_queue_size": 50000,
        "subscriptions_count": 2,
        "published_topics": "-",
    },
    "redpanda-connect-dynamodb": {
        "role": "Redpanda Connect -> DynamoDB NoSQL Sink",
        "network_zone": "Level 4 IT (it_dmz / it)",
        "mqtt_version": "MQTT v3.1.1",
        "clean_start": False,
        "session_expiry_sec": 4294967295,
        "max_queue_size": 50000,
        "subscriptions_count": 1,
        "published_topics": "-",
    },
    "nodered-asset3-consumer-app": {
        "role": "Node-RED Asset 3 Plant Consumer App",
        "network_zone": "Level 4 IT (it_dmz / it)",
        "mqtt_version": "MQTT v3.1.1",
        "clean_start": False,
        "session_expiry_sec": 4294967295,
        "max_queue_size": 50000,
        "subscriptions_count": 1,
        "published_topics": "-",
    },
    "mosquitto-uns-inspector": {
        "role": "Mosquitto Central UNS Topic & Metrics Inspector",
        "network_zone": "Level 3.5 / Level 4 (it_dmz / it)",
        "mqtt_version": "MQTT v3.1.1",
        "clean_start": True,
        "session_expiry_sec": 0,
        "max_queue_size": 1000,
        "subscriptions_count": 1,
        "published_topics": "-",
    },
}

KNOWN_SUBSCRIPTIONS = [
    {
        "subscription_id": "uns-enricher::raw",
        "client_id": "uns-enricher",
        "client_role": "In-UNS Schema Gatekeeper & MES Enricher",
        "topic_filter": "enterprise/plant/area/sub-area/cell/+/raw/",
        "qos": 1,
        "clean_session": False,
        "persistence_mode": "Persistent Session Queue (50,000 msgs)",
    },
    {
        "subscription_id": "uns-enricher::mes",
        "client_id": "uns-enricher",
        "client_role": "In-UNS Schema Gatekeeper & MES Enricher",
        "topic_filter": "enterprise/plant/area/sub-area/cell/mes/state/",
        "qos": 1,
        "clean_session": False,
        "persistence_mode": "Persistent Session Queue + Retained",
    },
    {
        "subscription_id": "timescaledb-group::telemetry",
        "client_id": "timescaledb-group",
        "client_role": "TimescaleDB Transactional Historian Sink",
        "topic_filter": "enterprise/plant/area/sub-area/cell/+/telemetry/",
        "qos": 1,
        "clean_session": False,
        "persistence_mode": "Persistent Session Queue (50,000 msgs)",
    },
    {
        "subscription_id": "timescaledb-group::dlq",
        "client_id": "timescaledb-group",
        "client_role": "TimescaleDB Transactional Historian Sink",
        "topic_filter": "enterprise/plant/area/sub-area/cell/+/dlq/",
        "qos": 1,
        "clean_session": False,
        "persistence_mode": "Persistent Session Queue (50,000 msgs)",
    },
    {
        "subscription_id": "redpanda-connect-dynamodb::telemetry",
        "client_id": "redpanda-connect-dynamodb",
        "client_role": "Redpanda Connect -> DynamoDB NoSQL Sink",
        "topic_filter": "enterprise/plant/area/sub-area/cell/+/telemetry/",
        "qos": 1,
        "clean_session": False,
        "persistence_mode": "Persistent Session Queue (50,000 msgs)",
    },
    {
        "subscription_id": "nodered-asset3-consumer-app::asset3",
        "client_id": "nodered-asset3-consumer-app",
        "client_role": "Node-RED Asset 3 Plant Consumer App",
        "topic_filter": "enterprise/plant/area/sub-area/cell/asset_3/telemetry/",
        "qos": 1,
        "clean_session": False,
        "persistence_mode": "Persistent Session Queue (50,000 msgs)",
    },
    {
        "subscription_id": "mosquitto-uns-inspector::all",
        "client_id": "mosquitto-uns-inspector",
        "client_role": "Mosquitto Central UNS Topic & Metrics Inspector",
        "topic_filter": "enterprise/#",
        "qos": 1,
        "clean_session": True,
        "persistence_mode": "Volatile Live Inspector",
    },
]


class TransactionalMQTTClient(mqtt.Client):
    """
    MQTT Client that withholds PUBACK when database insertion fails,
    allowing Mosquitto Central UNS to retain unacknowledged QoS 1 messages in its session queue.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.suppress_puback = False

    def _send_puback(self, mid):
        if self.suppress_puback:
            logger.warning(
                f"Withholding PUBACK for mid={mid} from Mosquitto Central UNS (TimescaleDB unavailable)"
            )
            return mqtt.MQTT_ERR_SUCCESS
        return super()._send_puback(mid)


def classify_topic(topic: str):
    parts = [p for p in topic.split("/") if p]
    asset_id = parts[-2] if len(parts) >= 2 else "unknown"
    leaf = parts[-1] if parts else "unknown"

    if leaf == "raw":
        return (
            "1_RAW_OT",
            asset_id,
            "edge-to-uns",
            ["uns-enricher"],
        )
    if leaf == "telemetry":
        subs = ["timescaledb-group", "redpanda-connect-dynamodb"]
        if asset_id == "asset_3":
            subs.append("nodered-asset3-consumer-app")
        return (
            "2_VALIDATED_TELEMETRY",
            asset_id,
            "uns-enricher",
            subs,
        )
    if leaf == "dlq":
        return (
            "4_DLQ_QUARANTINE",
            asset_id,
            "uns-enricher",
            ["timescaledb-group"],
        )
    if "mes/state" in topic:
        return (
            "3_MES_CONTEXT",
            "mes",
            "edge-to-uns",
            ["uns-enricher"],
        )
    return ("5_OTHER_UNS", asset_id, "unknown", [])


class MosquittoUnsInspector:
    """
    Observes Mosquitto Central UNS topics, subscriptions, and native event.log client lifecycle
    events, persisting them to TimescaleDB for the Mosquitto Central UNS Broker Inspector
    Grafana dashboard.
    """

    def __init__(self):
        self.db_conn = None
        self.lock = threading.Lock()
        self.topic_deltas = {}

    def get_db(self):
        if self.db_conn is not None and self.db_conn.closed == 0:
            return self.db_conn
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=4,
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SCHEMA_DDL)
            self.db_conn = conn
            self._seed_known_metadata()
            return self.db_conn
        except Exception as e:
            logger.debug(f"Inspector DB connection pending: {e}")
            self.db_conn = None
            return None

    def _seed_known_metadata(self):
        if not self.db_conn:
            return
        with self.db_conn.cursor() as cur:
            for cid, prof in KNOWN_CLIENT_PROFILES.items():
                cur.execute(
                    """
                    INSERT INTO uns_clients (
                        client_id, status, role, network_zone, mqtt_version,
                        clean_start, session_expiry_sec, max_queue_size,
                        subscriptions_count, published_topics
                    ) VALUES (%s, 'CONNECTED', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (client_id) DO UPDATE SET
                        role = EXCLUDED.role,
                        network_zone = EXCLUDED.network_zone,
                        mqtt_version = EXCLUDED.mqtt_version,
                        subscriptions_count = EXCLUDED.subscriptions_count,
                        published_topics = EXCLUDED.published_topics;
                    """,
                    (
                        cid,
                        prof["role"],
                        prof["network_zone"],
                        prof["mqtt_version"],
                        prof["clean_start"],
                        prof["session_expiry_sec"],
                        prof["max_queue_size"],
                        prof["subscriptions_count"],
                        prof["published_topics"],
                    ),
                )
            for sub in KNOWN_SUBSCRIPTIONS:
                cur.execute(
                    """
                    INSERT INTO uns_subscriptions (
                        subscription_id, client_id, client_role, client_status,
                        topic_filter, qos, clean_session, persistence_mode
                    ) VALUES (%s, %s, %s, 'CONNECTED', %s, %s, %s, %s)
                    ON CONFLICT (subscription_id) DO UPDATE SET
                        client_role = EXCLUDED.client_role,
                        topic_filter = EXCLUDED.topic_filter,
                        qos = EXCLUDED.qos,
                        clean_session = EXCLUDED.clean_session,
                        persistence_mode = EXCLUDED.persistence_mode;
                    """,
                    (
                        sub["subscription_id"],
                        sub["client_id"],
                        sub["client_role"],
                        sub["topic_filter"],
                        sub["qos"],
                        sub["clean_session"],
                        sub["persistence_mode"],
                    ),
                )

    def handle_uns_message(self, msg):
        topic = msg.topic
        if topic.startswith("$SYS"):
            return

        payload_bytes = msg.payload or b""
        byte_len = len(payload_bytes)
        payload_str = payload_bytes.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(payload_str)
            payload_json = json.dumps(parsed) if isinstance(parsed, dict) else json.dumps({"value": parsed})
        except Exception:
            payload_json = json.dumps({"raw_frame": payload_str[:256]})

        stage, asset_id, publisher_id, subscribers = classify_topic(topic)
        retained = bool(msg.retain) or ("mes/state" in topic)
        qos = int(msg.qos)

        matched_subs = []
        for sub in KNOWN_SUBSCRIPTIONS:
            if mqtt.topic_matches_sub(sub["topic_filter"], topic):
                matched_subs.append(
                    (sub["subscription_id"], sub["client_id"], sub["topic_filter"])
                )

        with self.lock:
            entry = self.topic_deltas.setdefault(
                topic,
                {"stage": stage, "asset_id": asset_id, "msgs": 0, "bytes": 0},
            )
            entry["msgs"] += 1
            entry["bytes"] += byte_len

        conn = self.get_db()
        if not conn:
            return

        try:
            with conn.cursor() as cur:
                # 1. Upsert uns_topics
                cur.execute(
                    """
                    INSERT INTO uns_topics (
                        topic, stage, asset_id, publisher_client_id, qos, retained,
                        subscriber_count, subscribers, message_count, total_bytes,
                        last_payload_bytes, first_seen, last_seen, last_payload
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, 1, %s,
                        %s, NOW(), NOW(), %s::jsonb
                    )
                    ON CONFLICT (topic) DO UPDATE SET
                        stage = EXCLUDED.stage,
                        asset_id = EXCLUDED.asset_id,
                        publisher_client_id = EXCLUDED.publisher_client_id,
                        qos = EXCLUDED.qos,
                        retained = uns_topics.retained OR EXCLUDED.retained,
                        subscriber_count = EXCLUDED.subscriber_count,
                        subscribers = EXCLUDED.subscribers,
                        message_count = uns_topics.message_count + 1,
                        total_bytes = uns_topics.total_bytes + EXCLUDED.last_payload_bytes,
                        last_payload_bytes = EXCLUDED.last_payload_bytes,
                        last_seen = NOW(),
                        last_payload = EXCLUDED.last_payload;
                    """,
                    (
                        topic,
                        stage,
                        asset_id,
                        publisher_id,
                        qos,
                        retained,
                        len(subscribers),
                        ", ".join(subscribers),
                        byte_len,
                        byte_len,
                        payload_json,
                    ),
                )

                # 2. Update publisher messages_in (into broker from client)
                cur.execute(
                    """
                    UPDATE uns_clients
                    SET messages_in = messages_in + 1,
                        last_seen = NOW()
                    WHERE client_id = %s;
                    """,
                    (publisher_id,),
                )

                # 3. Update matching subscriptions & subscriber messages_out (from broker to client)
                for sub_id, sub_client_id, sub_filter in matched_subs:
                    cur.execute(
                        """
                        UPDATE uns_subscriptions
                        SET routed_messages = routed_messages + 1,
                            routed_bytes = routed_bytes + %s,
                            last_delivery = NOW(),
                            matched_topics_count = (
                                SELECT COUNT(*) FROM uns_topics
                                WHERE (
                                    (%s LIKE '%%/+/raw/' AND topic LIKE '%%/raw/')
                                    OR (%s LIKE '%%/+/telemetry/' AND topic LIKE '%%/telemetry/')
                                    OR (%s LIKE '%%/+/dlq/' AND topic LIKE '%%/dlq/')
                                    OR (%s = 'enterprise/#' AND topic LIKE 'enterprise/%%')
                                    OR topic = %s
                                )
                            )
                        WHERE subscription_id = %s;
                        """,
                        (
                            byte_len,
                            sub_filter,
                            sub_filter,
                            sub_filter,
                            sub_filter,
                            sub_filter,
                            sub_id,
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE uns_clients
                        SET messages_out = messages_out + 1,
                            last_seen = NOW()
                        WHERE client_id = %s;
                        """,
                        (sub_client_id,),
                    )
        except Exception as e:
            logger.warning(f"Inspector DB update error: {e}")
            try:
                conn.close()
            except Exception:
                pass
            self.db_conn = None

    def parse_event_line(self, line: str):
        line = line.strip()
        if not line:
            return
        conn = self.get_db()
        if not conn:
            return

        # Mosquitto log patterns:
        # 2026-10-29 17:15:23: New client connected from 172.20.0.4:41234 as uns-enricher (p2, c0, k30).
        # 2026-10-29 17:16:00: Client uns-enricher disconnected.
        m_mosq_conn = re.search(
            r"New client connected from\s+([0-9a-fA-F\.:]+)(?::\d+)?\s+as\s+(\S+?)(?:\s*\((?:p\d+,\s*)?c([01])(?:,\s*k\d+)?\))?\.",
            line,
            re.IGNORECASE,
        )
        m_mosq_disc = re.search(
            r"Client\s+(\S+)\s+(?:has\s+)?(?:disconnected|closed its connection)(.*)$",
            line,
            re.IGNORECASE,
        )



        is_conn = False
        is_disc = False
        cid = None
        ip_addr = "-"
        clean_st = False
        sess_exp = 4294967295
        reason_tail = ""

        if m_mosq_conn:
            cid = m_mosq_conn.group(2).strip()
            ip_addr = m_mosq_conn.group(1).strip()
            clean_st = (m_mosq_conn.group(3) == "1") if m_mosq_conn.group(3) else False
            sess_exp = 0 if clean_st else 4294967295
            is_conn = True

        elif m_mosq_disc:
            cid = m_mosq_disc.group(1).strip()
            reason_tail = m_mosq_disc.group(2).strip()
            is_disc = True


        if cid and cid.upper() == "UNKNOWN":
            return

        try:
            with conn.cursor() as cur:
                if is_conn and cid:
                    prof = KNOWN_CLIENT_PROFILES.get(
                        cid,
                        {
                            "role": "Grafana Live MQTT / Dynamic UNS Client"
                            if "grafana" in cid.lower() or "mqtt" in cid.lower()
                            else "Dynamic MQTT Client",
                            "network_zone": "Level 3.5 / Level 4 (it_dmz)",
                            "mqtt_version": "MQTT v3.1.1 / v5.0",
                            "max_queue_size": 1000 if clean_st else 50000,
                            "subscriptions_count": 1,
                            "published_topics": "-",
                        },
                    )
                    cur.execute(
                        """
                        INSERT INTO uns_clients (
                            client_id, status, role, network_zone, ip_address, mqtt_version,
                            clean_start, session_expiry_sec, max_queue_size,
                            subscriptions_count, published_topics, connected_at, last_seen
                        ) VALUES (%s, 'CONNECTED', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (client_id) DO UPDATE SET
                            status = 'CONNECTED',
                            ip_address = EXCLUDED.ip_address,
                            clean_start = EXCLUDED.clean_start,
                            session_expiry_sec = EXCLUDED.session_expiry_sec,
                            connected_at = NOW(),
                            last_seen = NOW();
                        """,
                        (
                            cid,
                            prof["role"],
                            prof["network_zone"],
                            ip_addr,
                            prof["mqtt_version"],
                            clean_st,
                            sess_exp,
                            prof["max_queue_size"],
                            prof["subscriptions_count"],
                            prof["published_topics"],
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE uns_subscriptions
                        SET client_status = 'CONNECTED'
                        WHERE client_id = %s;
                        """,
                        (cid,),
                    )
                    cur.execute(
                        """
                        INSERT INTO uns_client_events (
                            time, client_id, event_type, ip_address, clean_start, session_expiry, details
                        ) VALUES (NOW(), %s, 'CLIENT_CONNECTED', %s, %s, %s, %s);
                        """,
                        (cid, ip_addr, str(clean_st), str(sess_exp), line),
                    )
                elif is_disc and cid:
                    cur.execute(
                        """
                        UPDATE uns_clients
                        SET status = 'DISCONNECTED',
                            ip_address = CASE WHEN %s != '-' THEN %s ELSE ip_address END,
                            last_seen = NOW()
                        WHERE client_id = %s;
                        """,
                        (ip_addr, ip_addr, cid),
                    )
                    cur.execute(
                        """
                        UPDATE uns_subscriptions
                        SET client_status = CASE
                            WHEN clean_session = FALSE THEN 'OFFLINE (Queuing in UNS Broker)'
                            ELSE 'DISCONNECTED'
                        END
                        WHERE client_id = %s;
                        """,
                        (cid,),
                    )
                    cur.execute(
                        """
                        INSERT INTO uns_client_events (
                            time, client_id, event_type, ip_address, clean_start, session_expiry, details
                        ) VALUES (NOW(), %s, 'CLIENT_DISCONNECTED', %s, '-', '-', %s);
                        """,
                        (cid, ip_addr, f"Disconnected {reason_tail}".strip()),
                    )
        except Exception as e:
            logger.debug(f"Event log DB write failed: {e}")

    def tail_event_log_loop(self):
        pos = 0
        while True:
            try:
                if os.path.exists(BROKER_LOG):
                    file_size = os.path.getsize(BROKER_LOG)
                    if file_size < pos:
                        pos = 0
                    if file_size > pos:
                        with open(BROKER_LOG, "r", encoding="utf-8", errors="replace") as f:
                            f.seek(pos)
                            for line in f:
                                self.parse_event_line(line)
                            pos = f.tell()
            except Exception as e:
                logger.debug(f"Error tailing {BROKER_LOG}: {e}")
            time.sleep(2.0)

    def flush_metrics_loop(self):
        while True:
            time.sleep(5.0)
            with self.lock:
                snapshot = self.topic_deltas
                self.topic_deltas = {}

            if not snapshot:
                continue

            conn = self.get_db()
            if not conn:
                continue

            try:
                with conn.cursor() as cur:
                    for topic, info in snapshot.items():
                        cur.execute(
                            """
                            INSERT INTO uns_topic_metrics (
                                time, topic, stage, asset_id, messages_delta, bytes_delta,
                                total_messages
                            ) VALUES (
                                NOW(), %s, %s, %s, %s, %s,
                                COALESCE((SELECT message_count FROM uns_topics WHERE topic = %s), %s)
                            );
                            """,
                            (
                                topic,
                                info["stage"],
                                info["asset_id"],
                                info["msgs"],
                                info["bytes"],
                                topic,
                                info["msgs"],
                            ),
                        )
            except Exception as e:
                logger.debug(f"Metrics flush error: {e}")

    def start_background_threads(self):
        threading.Thread(target=self.tail_event_log_loop, daemon=True).start()
        threading.Thread(target=self.flush_metrics_loop, daemon=True).start()
        threading.Thread(target=self._run_mqtt_inspector_loop, daemon=True).start()

    def _run_mqtt_inspector_loop(self):
        while True:
            try:
                client = mqtt.Client(
                    client_id="mosquitto-uns-inspector",
                    clean_session=True,
                    protocol=mqtt.MQTTv311,
                )

                def on_connect(c, userdata, flags, rc):
                    if rc == 0:
                        logger.info(
                            f"Mosquitto UNS Inspector connected to {MQTT_HOST}:{MQTT_PORT} (subscribing to enterprise/#)"
                        )
                        c.subscribe("enterprise/#", qos=1)

                def on_message(c, userdata, msg):
                    self.handle_uns_message(msg)

                client.on_connect = on_connect
                client.on_message = on_message
                client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
                client.loop_forever()
            except Exception as e:
                logger.debug(f"Mosquitto UNS Inspector loop retry: {e}")
                time.sleep(3.0)


class MqttToTimescaleSink:
    def __init__(self):
        self.db_conn = None

    def ensure_db_connection(self) -> bool:
        if self.db_conn is not None and self.db_conn.closed == 0:
            return True
        try:
            self.db_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=4,
            )
            self.db_conn.autocommit = True
            with self.db_conn.cursor() as cur:
                cur.execute(SCHEMA_DDL)
            logger.info(
                f"Connected to TimescaleDB at {DB_HOST}:{DB_PORT}/{DB_NAME} (tables verified)"
            )
            return True
        except Exception as e:
            logger.warning(f"TimescaleDB ({DB_HOST}:{DB_PORT}) unreachable: {e}")
            self.db_conn = None
            return False

    def insert_dlq(self, topic: str, payload_bytes: bytes) -> bool:
        if not self.ensure_db_connection():
            return False

        payload_str = payload_bytes.decode("utf-8", errors="replace")
        event_time = datetime.now(timezone.utc).isoformat()
        asset_id = "unknown"
        error_type = "SchemaValidationError"
        failed_field = "root"
        reason = "Validation failed"

        try:
            data = json.loads(payload_str)
            if isinstance(data, dict):
                event_time = data.get("rejected_at") or event_time
                asset_id = str(data.get("asset_id", "unknown"))
                error_type = str(data.get("error_type", "SchemaValidationError"))
                failed_field = str(data.get("failed_field", "root"))
                reason = str(data.get("reason", "Validation failed"))
                payload_str = json.dumps(data)
        except Exception:
            payload_str = json.dumps({"raw": payload_str, "topic": topic})

        try:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO asset_dlq (time, asset_id, error_type, failed_field, reason, payload)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb);
                    """,
                    (event_time, asset_id, error_type, failed_field, reason, payload_str),
                )
            logger.info(
                f"Inserted DLQ violation from '{topic}' (asset={asset_id}, field={failed_field}) into asset_dlq"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to insert DLQ record into TimescaleDB: {e}")
            try:
                if self.db_conn:
                    self.db_conn.close()
            except Exception:
                pass
            self.db_conn = None
            return False

    def insert_telemetry(self, topic: str, payload_bytes: bytes) -> bool:
        if "/dlq" in topic:
            return self.insert_dlq(topic, payload_bytes)

        if not self.ensure_db_connection():
            return False

        payload_str = payload_bytes.decode("utf-8", errors="replace")
        event_time = datetime.now(timezone.utc).isoformat()

        try:
            data = json.loads(payload_str)
            if isinstance(data, dict):
                if "topic" not in data:
                    data["topic"] = topic
                if "timestamp" in data and data["timestamp"]:
                    event_time = data["timestamp"]
                payload_str = json.dumps(data)
            else:
                payload_str = json.dumps({"value": data, "topic": topic})
        except Exception:
            payload_str = json.dumps({"raw": payload_str, "topic": topic})

        try:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO asset_telemetry (time, payload)
                    VALUES (%s, %s::jsonb);
                    """,
                    (event_time, payload_str),
                )
            logger.info(f"Inserted telemetry from '{topic}' at {event_time} into TimescaleDB")
            return True
        except Exception as e:
            logger.error(f"Failed to insert telemetry into TimescaleDB: {e}")
            try:
                if self.db_conn:
                    self.db_conn.close()
            except Exception:
                pass
            self.db_conn = None
            return False

    def run_forever(self):
        logger.info(
            f"Starting Mosquitto-UNS-to-TimescaleDB Sink: "
            f"{MQTT_HOST}:{MQTT_PORT} ({MQTT_TOPIC} & {DLQ_TOPIC}) -> {DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        inspector = MosquittoUnsInspector()
        inspector.start_background_threads()

        while True:
            while not self.ensure_db_connection():
                time.sleep(3.0)

            client = TransactionalMQTTClient(
                client_id=MQTT_CLIENT_ID,
                clean_session=False,
            )
            client.max_inflight_messages_set(1)

            def on_connect(c, userdata, flags, rc):
                if rc == 0:
                    logger.info(
                        f"Connected to Mosquitto Central UNS ({MQTT_HOST}:{MQTT_PORT}) with persistent session "
                        f"(client_id='{MQTT_CLIENT_ID}', clean_session=False)"
                    )
                    c.subscribe([(MQTT_TOPIC, 1), (DLQ_TOPIC, 1)])
                else:
                    logger.error(f"Mosquitto Central UNS connection failed with rc={rc}")

            def on_message(c: TransactionalMQTTClient, userdata, msg):
                if msg.topic.startswith("$SYS"):
                    return
                c.suppress_puback = False
                ok = self.insert_telemetry(msg.topic, msg.payload)
                if not ok:
                    c.suppress_puback = True
                    c.disconnect()

            client.on_connect = on_connect
            client.on_message = on_message

            try:
                client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
                client.loop_forever()
            except Exception as e:
                logger.warning(f"Mosquitto Central UNS consumer loop interrupted: {e}")

            logger.info("Sink paused; waiting 2s before retrying TimescaleDB & Mosquitto Central UNS...")
            time.sleep(2.0)


if __name__ == "__main__":
    sink = MqttToTimescaleSink()
    sink.run_forever()
