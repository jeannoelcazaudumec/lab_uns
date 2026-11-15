import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from jsonschema import Draft202012Validator
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("uns_enricher")

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto-uns")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "uns-enricher")

MES_STATE_TOPIC = os.getenv(
    "MES_STATE_TOPIC", "enterprise/plant/area/sub-area/cell/mes/state/"
)
RAW_TELEMETRY_TOPIC = os.getenv(
    "RAW_TELEMETRY_TOPIC", "enterprise/plant/area/sub-area/cell/+/raw/"
)
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "/app/schemas/telemetry_schema.json")

running = True

# Active production context held in the UNS Enricher state
production_context = {
    "part_number": "PN-1001-STEEL",
    "batch_id": "LOT-2026-01",
    "mes_updated_at": None,
}

# Load and compile canonical UNS JSON Schema (Draft 2020-12)
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    TELEMETRY_SCHEMA = json.load(f)

Draft202012Validator.check_schema(TELEMETRY_SCHEMA)
schema_validator = Draft202012Validator(TELEMETRY_SCHEMA)
logger.info(f"Loaded canonical UNS JSON Schema from {SCHEMA_PATH}")


def handle_sigterm(signum, frame):
    global running
    logger.info("Received stop signal, shutting down uns-enricher...")
    running = False


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def extract_asset_from_topic(topic: str) -> str:
    parts = [p for p in topic.split("/") if p]
    if len(parts) >= 2 and parts[-1] == "raw":
        return parts[-2]
    return "unknown_asset"


def extract_failed_field(err) -> str:
    if err.path:
        return ".".join(str(p) for p in err.path)
    if err.validator == "required":
        m = re.search(r"'([^']+)' is a required property", err.message)
        if m:
            return m.group(1)
    if err.validator == "additionalProperties":
        m = re.search(r"\('([^']+)' was unexpected\)", err.message)
        if m:
            return m.group(1)
    return "root"


def publish_dlq(
    client: mqtt.Client,
    source_topic: str,
    asset_id: str,
    error_type: str,
    failed_field: str,
    schema_rule: str,
    reason: str,
    raw_payload,
):
    dlq_topic = (
        source_topic.replace("/raw", "/dlq")
        if "/raw" in source_topic
        else f"enterprise/plant/area/sub-area/cell/{asset_id}/dlq/"
    )
    dlq_event = {
        "rejected_at": datetime.now(timezone.utc).isoformat(),
        "asset_id": asset_id,
        "source_topic": source_topic,
        "dlq_topic": dlq_topic,
        "error_type": error_type,
        "failed_field": failed_field,
        "schema_rule": schema_rule,
        "reason": reason,
        "active_part_number": production_context["part_number"],
        "raw_payload": raw_payload,
    }
    info = client.publish(
        dlq_topic, payload=json.dumps(dlq_event), qos=1, retain=False
    )
    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        logger.warning(
            f"[SCHEMA REJECTED -> DLQ] asset={asset_id} error={error_type} "
            f"field='{failed_field}' rule='{schema_rule}' reason=\"{reason}\" -> {dlq_topic}"
        )
    else:
        logger.error(f"Failed to publish DLQ event to {dlq_topic}: rc={info.rc}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        session_present = flags.get("session present", 0)
        logger.info(
            f"Connected to Mosquitto Central UNS ({MQTT_HOST}:{MQTT_PORT}) as '{MQTT_CLIENT_ID}' "
            f"(clean_session=False, session_present={session_present})"
        )
        # Subscribe to MES production state topic (Retained in Mosquitto UNS) and raw asset topics
        client.subscribe([(MES_STATE_TOPIC, 1), (RAW_TELEMETRY_TOPIC, 1)])
        logger.info(
            f"Subscribed to MES state '{MES_STATE_TOPIC}' and raw stream '{RAW_TELEMETRY_TOPIC}'"
        )
    else:
        logger.error(f"Failed to connect to Mosquitto Central UNS, return code {rc}")


def on_message(client, userdata, msg):
    payload_str = msg.payload.decode("utf-8", errors="replace")

    try:
        data = json.loads(payload_str)
    except Exception as e:
        if "/raw" in msg.topic:
            asset_id = extract_asset_from_topic(msg.topic)
            publish_dlq(
                client=client,
                source_topic=msg.topic,
                asset_id=asset_id,
                error_type="MalformedJSONError",
                failed_field="root",
                schema_rule="json_parse",
                reason=f"Payload is not valid JSON: {e}",
                raw_payload=payload_str,
            )
        else:
            logger.error(f"Invalid JSON on topic {msg.topic}: {e}")
        return

    # 1. Handle MES_Simul Production State Update Event
    if msg.topic.rstrip("/") == MES_STATE_TOPIC.rstrip("/"):
        if isinstance(data, dict):
            new_part = data.get("part_number")
            new_batch = data.get("batch_id", "LOT-2026-01")
            if new_part:
                production_context["part_number"] = new_part
                production_context["batch_id"] = new_batch
                production_context["mes_updated_at"] = data.get("timestamp")
                logger.info(
                    f"[MES CONTEXT UPDATED] Active Part Number: {new_part} | Batch: {new_batch}"
                )
        return

    # 2. Handle Raw Asset Telemetry -> Validate Schema -> Augment -> Publish to .../telemetry/ or .../dlq/
    if "/raw" in msg.topic:
        topic_asset_id = extract_asset_from_topic(msg.topic)
        asset_id = (
            data.get("asset_id", topic_asset_id)
            if isinstance(data, dict)
            else topic_asset_id
        )

        # Validate against canonical JSON Schema (O(1) early-exit)
        first_err = next(schema_validator.iter_errors(data), None)
        if first_err is not None:
            failed_field = extract_failed_field(first_err)
            publish_dlq(
                client=client,
                source_topic=msg.topic,
                asset_id=str(asset_id),
                error_type="SchemaValidationError",
                failed_field=failed_field,
                schema_rule=str(first_err.validator),
                reason=first_err.message,
                raw_payload=data,
            )
            return

        # Schema Passed -> Enrich with MES Production Context and publish to .../telemetry/
        enriched_topic = msg.topic.replace("/raw", "/telemetry")
        data["part_number"] = production_context["part_number"]
        data["batch_id"] = production_context["batch_id"]
        data["schema_valid"] = True
        data["topic"] = enriched_topic

        enriched_payload = json.dumps(data)
        info = client.publish(
            enriched_topic, payload=enriched_payload, qos=1, retain=False
        )
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(
                f"[UNS VALIDATED & AUGMENTED] asset={data.get('asset_id')} "
                f"part_number={data['part_number']} batch={data['batch_id']} -> {enriched_topic}"
            )
        else:
            logger.error(
                f"Failed to publish enriched message to {enriched_topic}: rc={info.rc}"
            )


def main():
    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID, clean_session=False, protocol=mqtt.MQTTv311
    )
    client.on_connect = on_connect
    client.on_message = on_message

    while running:
        try:
            logger.info(f"Connecting to Mosquitto UNS broker at {MQTT_HOST}:{MQTT_PORT}...")
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            client.loop_forever()
        except Exception as e:
            if running:
                logger.warning(f"Connection interrupted ({e}). Retrying in 2s...")
                time.sleep(2)

    try:
        client.disconnect()
    except Exception:
        pass
    logger.info("uns-enricher stopped cleanly.")


if __name__ == "__main__":
    main()
