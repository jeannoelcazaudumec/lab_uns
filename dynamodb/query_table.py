import json
import os
import urllib.request

DYNAMODB_ENDPOINT = os.getenv("DYNAMODB_ENDPOINT", "http://localhost:8000")
TABLE_NAME = os.getenv("DYNAMODB_TABLE", "asset_telemetry")

HEADERS = {
    "Content-Type": "application/x-amz-json-1.0",
    "X-Amz-Date": "20261025T000000Z",
    "Authorization": (
        "AWS4-HMAC-SHA256 Credential=local/20261025/us-east-1/dynamodb/aws4_request, "
        "SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=0000"
    ),
}


def dynamo_request(target: str, payload: dict) -> dict:
    headers = dict(HEADERS)
    headers["X-Amz-Target"] = f"DynamoDB_20120810.{target}"
    req = urllib.request.Request(
        DYNAMODB_ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def unwrap_av(av: dict):
    if "S" in av:
        return av["S"]
    if "N" in av:
        return float(av["N"]) if "." in av["N"] else int(av["N"])
    if "M" in av:
        return {k: unwrap_av(v) for k, v in av["M"].items()}
    if "L" in av:
        return [unwrap_av(x) for x in av["L"]]
    if "BOOL" in av:
        return av["BOOL"]
    if "NULL" in av:
        return None
    return av


def main():
    desc = dynamo_request("DescribeTable", {"TableName": TABLE_NAME})["Table"]
    scan = dynamo_request("Scan", {"TableName": TABLE_NAME, "Limit": 10})
    items = [unwrap_av({"M": item}) for item in scan.get("Items", [])]
    items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    print(f"=== DynamoDB Table: {TABLE_NAME} | Total Items: {len(items)} (Status: {desc['TableStatus']}) ===")
    for item in items[:10]:
        payload = item.get("payload", {})
        part_no = item.get("part_number") or payload.get("part_number", "-")
        print(
            f"[{item.get('timestamp')}] asset={item.get('asset_id')} "
            f"part={part_no} device={item.get('device')} "
            f"temp={payload.get('temperature')}°C "
            f"hum={payload.get('humidity')}% rpm={payload.get('motor_speed', '-')}"
        )


if __name__ == "__main__":
    main()
