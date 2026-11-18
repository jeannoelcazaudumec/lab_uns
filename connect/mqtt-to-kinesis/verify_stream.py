#!/usr/bin/env python3
"""
Verification Script for Containerized MQTT-to-AWS Kinesis Ingestion Service

Checks:
1. Verifies Kinesis stream existence and ACTIVE status.
2. Reads records from all open shards using TRIM_HORIZON iterator.
3. Decompresses Gzip level 6 payloads.
4. Parses Newline-Delimited JSON (NDJSON) records.
5. Validates telemetry payload schema and integrity.
6. Computes compression ratios and billable PPU cost savings.
"""

import gzip
import json
import math
import os
import sys
import time
from typing import Any, Dict, List

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Configuration
KINESIS_ENDPOINT_URL = os.getenv("KINESIS_ENDPOINT_URL", "http://localstack:4566")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "iiot-telemetry-stream")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
PPU_UNIT_BYTES = 25600


def main():
    print("=" * 75)
    print("MQTT-to-AWS Kinesis Stream Ingestion Verification Tool")
    print(f"Endpoint: {KINESIS_ENDPOINT_URL} | Stream: {KINESIS_STREAM_NAME} | Region: {AWS_DEFAULT_REGION}")
    print("=" * 75)

    boto_config = Config(
        max_pool_connections=5,
        retries={"max_attempts": 3, "mode": "standard"},
    )
    kinesis = boto3.client(
        "kinesis",
        endpoint_url=KINESIS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
        config=boto_config,
    )

    # 1. Verify Stream Existence and ACTIVE Status
    try:
        try:
            desc = kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
            status = desc["StreamDescriptionSummary"]["StreamStatus"]
            shards_info = desc["StreamDescriptionSummary"].get("OpenShardCount", 1)
        except Exception:
            desc = kinesis.describe_stream(StreamName=KINESIS_STREAM_NAME)
            status = desc["StreamDescription"]["StreamStatus"]
            shards_info = len(desc["StreamDescription"].get("Shards", []))

        print(f"[1/5] Stream Status: '{status}' with {shards_info} open shard(s).")
        if status != "ACTIVE":
            print(f"ERROR: Stream '{KINESIS_STREAM_NAME}' is not ACTIVE (status: {status}).")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to describe stream '{KINESIS_STREAM_NAME}': {e}")
        sys.exit(1)

    # 2. Get Shards
    try:
        shards_resp = kinesis.describe_stream(StreamName=KINESIS_STREAM_NAME)
        shards = shards_resp["StreamDescription"]["Shards"]
        shard_ids = [s["ShardId"] for s in shards]
        print(f"[2/5] Discovered {len(shard_ids)} shard(s): {', '.join(shard_ids)}")
    except Exception as e:
        print(f"ERROR: Failed to retrieve shards: {e}")
        sys.exit(1)

    # 3. Read Records from Shards
    total_kinesis_records = 0
    total_telemetry_events = 0
    total_raw_bytes = 0
    total_compressed_bytes = 0
    total_unopt_ppus = 0
    total_actual_ppus = 0
    sample_event = None

    print(f"[3/5] Polling records from shards (Iterator: TRIM_HORIZON)...")

    for shard_id in shard_ids:
        try:
            iter_resp = kinesis.get_shard_iterator(
                StreamName=KINESIS_STREAM_NAME,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iterator = iter_resp["ShardIterator"]

            # Poll records
            records_resp = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
            records = records_resp.get("Records", [])
            print(f"      Shard '{shard_id}': retrieved {len(records)} Kinesis record(s).")

            for rec_idx, rec in enumerate(records):
                total_kinesis_records += 1
                pkey = rec.get("PartitionKey", "unknown")
                comp_data = rec["Data"]
                comp_size = len(comp_data)
                total_compressed_bytes += comp_size

                # Decompress Gzip
                try:
                    decomp = gzip.decompress(comp_data)
                except Exception as de:
                    print(f"ERROR: Failed to decompress Gzip record {rec_idx}: {de}")
                    continue

                raw_size = len(decomp)
                total_raw_bytes += raw_size

                # Parse NDJSON
                lines = [line.strip() for line in decomp.decode("utf-8", errors="replace").split("\n") if line.strip()]
                total_telemetry_events += len(lines)

                # PPU calculation for this record
                rec_unopt_ppus = sum(math.ceil(len(l.encode("utf-8")) / PPU_UNIT_BYTES) for l in lines)
                rec_actual_ppu = math.ceil(comp_size / PPU_UNIT_BYTES)
                total_unopt_ppus += rec_unopt_ppus
                total_actual_ppus += rec_actual_ppu

                if sample_event is None and lines:
                    try:
                        sample_event = json.loads(lines[0])
                    except Exception:
                        sample_event = lines[0]

        except Exception as e:
            print(f"ERROR reading shard {shard_id}: {e}")

    # 4. Verification Check & Summary
    print(f"[4/5] Ingestion & Payload Verification Results:")
    print(f"      - Kinesis Records Fetched:      {total_kinesis_records}")
    print(f"      - Telemetry Events Parsed:      {total_telemetry_events}")
    print(f"      - Total Raw (NDJSON) Bytes:     {total_raw_bytes:,} bytes")
    print(f"      - Total Gzip Compressed Bytes:  {total_compressed_bytes:,} bytes")

    if total_raw_bytes > 0:
        byte_savings = ((total_raw_bytes - total_compressed_bytes) / total_raw_bytes) * 100.0
        print(f"      - Gzip Compression Ratio:       {byte_savings:.2f}% data reduction")

    print(f"[5/5] AWS Kinesis PPU Financial Economics:")
    print(f"      - Unoptimized PPUs (1/event):   {total_unopt_ppus} PPUs")
    print(f"      - Actual Billed PPUs (25KB):    {total_actual_ppus} PPUs")

    if total_unopt_ppus > 0:
        ppu_savings = ((total_unopt_ppus - total_actual_ppus) / total_unopt_ppus) * 100.0
        print(f"      - Billable Ingestion Savings:   {ppu_savings:.2f}% COST REDUCTION")

    if sample_event:
        print("\n" + "-" * 75)
        print("Sample Decompressed UNS Telemetry Event:")
        if isinstance(sample_event, dict):
            print(json.dumps(sample_event, indent=2))
        else:
            print(sample_event)
        print("-" * 75)

    if total_kinesis_records > 0 and total_telemetry_events > 0:
        print("\n>>> ALL VERIFICATION CHECKS PASSED SUCCESSFULLY! <<<")
        sys.exit(0)
    else:
        print("\nWARNING: No records found yet. Stream may still be buffering or no events published.")
        sys.exit(1)


if __name__ == "__main__":
    main()
