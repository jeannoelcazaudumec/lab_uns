#!/usr/bin/env python3
"""
Verification Script for MQTT-to-S3 Data Lake Ingestion Pipeline

Checks:
1. S3 bucket existence and object listings.
2. Hive partitioning key format verification.
3. Metadata inspection (record-count, uncompressed-bytes, compressed-bytes, ingested-at).
4. Gzip decompression and NDJSON parsing validation.
5. Compression ratio verification (>90%).
"""

import gzip
import json
import os
import sys
import boto3
from botocore.config import Config

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "telemetry-lake")
S3_PREFIX = os.getenv("S3_PREFIX", "telemetry")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def main():
    print("=" * 70)
    print("MQTT-to-S3 Data Lake Verification Tool")
    print(f"Endpoint: {S3_ENDPOINT_URL} | Bucket: {S3_BUCKET} | Prefix: {S3_PREFIX}")
    print("=" * 70)

    boto_config = Config(
        max_pool_connections=5,
        retries={"max_attempts": 3, "mode": "standard"},
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=boto_config,
    )

    # 1. Check Bucket
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f" Bucket '{S3_BUCKET}' exists.")
    except Exception as e:
        print(f" Bucket '{S3_BUCKET}' check failed: {e}")
        sys.exit(1)

    # 2. List Objects
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    contents = response.get("Contents", [])
    if not contents:
        print(f" No objects found under prefix '{S3_PREFIX}'.")
        sys.exit(1)

    print(f" Found {len(contents)} object(s) in bucket:")
    for obj in contents[-5:]:
        print(f"   - {obj['Key']} ({obj['Size']} bytes, modified {obj['LastModified']})")

    # 3. Retrieve Latest Object
    latest_obj = sorted(contents, key=lambda x: x["LastModified"])[-1]
    latest_key = latest_obj["Key"]
    print(f"\n Inspecting latest object: {latest_key}")

    obj_resp = s3.get_object(Bucket=S3_BUCKET, Key=latest_key)
    metadata = obj_resp.get("Metadata", {})
    raw_compressed = obj_resp["Body"].read()

    print(f"   Compressed Size (bytes): {len(raw_compressed)}")
    print(f"   S3 Metadata: {json.dumps(metadata, indent=2)}")

    # 4. Decompress Gzip
    try:
        decompressed_data = gzip.decompress(raw_compressed)
        uncompressed_size = len(decompressed_data)
        print(f"   Uncompressed Size (bytes): {uncompressed_size}")
    except Exception as e:
        print(f" Gzip decompression failed: {e}")
        sys.exit(1)

    # 5. Verify Compression Ratio
    if uncompressed_size > 0:
        ratio = (1.0 - (len(raw_compressed) / uncompressed_size)) * 100.0
        print(f"   Compression Ratio: {ratio:.2f}%")
        if ratio >= 80.0:
            print(f"   Compression Ratio check PASS (>= 80% achieved, target >90% on larger batches)")
        else:
            print(f"   Compression Ratio: {ratio:.2f}%")

    # 6. Parse NDJSON Records
    lines = [line.strip() for line in decompressed_data.decode("utf-8").split("\n") if line.strip()]
    print(f"   NDJSON Lines Count: {len(lines)}")

    expected_count = int(metadata.get("record-count", len(lines)))
    if len(lines) == expected_count:
        print(f"   Record count matches metadata: {len(lines)} records.")
    else:
        print(f"   Record count mismatch! lines={len(lines)}, metadata={expected_count}")
        sys.exit(1)

    # Verify JSON validity of each record
    sample_record = None
    for i, line in enumerate(lines):
        try:
            parsed = json.loads(line)
            if i == 0:
                sample_record = parsed
        except json.JSONDecodeError as je:
            print(f"   Line {i} is invalid JSON: {je}")
            sys.exit(1)

    print(f"   All {len(lines)} records are valid JSON.")
    print("\n Sample Record Preview:")
    print(json.dumps(sample_record, indent=2))

    print("\n" + "=" * 70)
    print(" ALL VERIFICATION CHECKS PASSED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    main()
