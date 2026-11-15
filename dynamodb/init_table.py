import json
import os
import sys
import time
import urllib.error
import urllib.request

DYNAMODB_ENDPOINT = os.getenv("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000")
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
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    print(f"Waiting for DynamoDB Local at {DYNAMODB_ENDPOINT}...")
    while True:
        try:
            tables = dynamo_request("ListTables", {}).get("TableNames", [])
            break
        except Exception as e:
            print(f"DynamoDB Local not ready yet ({e}), retrying in 2s...")
            time.sleep(2)

    if TABLE_NAME in tables:
        print(f"DynamoDB table '{TABLE_NAME}' already exists.")
        return

    print(f"Creating DynamoDB table '{TABLE_NAME}' (PK: asset_id [S], SK: timestamp [S])...")
    create_payload = {
        "TableName": TABLE_NAME,
        "KeySchema": [
            {"AttributeName": "asset_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "asset_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    try:
        dynamo_request("CreateTable", create_payload)
        print(f"Successfully created DynamoDB table '{TABLE_NAME}'.")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        if "ResourceInUseException" in body:
            print(f"Table '{TABLE_NAME}' already exists.")
        else:
            print(f"Failed to create table: {e.code} {body}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
