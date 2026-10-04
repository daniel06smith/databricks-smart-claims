"""Replay the course's telematics Parquet data into a Kinesis data stream.

Run in AWS CloudShell from the folder containing the cloned repo:
    python3 telematics_producer.py --limit 5000
"""
import argparse
import glob
import json
import time

import boto3
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--stream", default="telematics-stream-tmh")
parser.add_argument("--region", default="us-east-1")
parser.add_argument("--data-dir", default="databricks-zero-to-hero-course/data/telematics")
parser.add_argument("--limit", type=int, default=5000, help="rows to send (0 = all ~780k)")
parser.add_argument("--rate", type=int, default=500, help="max records per second")
args = parser.parse_args()

files = sorted(glob.glob(f"{args.data_dir}/*.parquet"))
if not files:
    raise SystemExit(f"No parquet files found in {args.data_dir}")

df = pd.concat(pd.read_parquet(f) for f in files).sort_values("event_timestamp")
if args.limit:
    df = df.head(args.limit)
print(f"Sending {len(df):,} records to {args.stream} ({args.region})")

kinesis = boto3.client("kinesis", region_name=args.region)
BATCH_SIZE = 500  # PutRecords accepts at most 500 records per call
sent = 0

for start in range(0, len(df), BATCH_SIZE):
    rows = df.iloc[start:start + BATCH_SIZE].to_dict("records")
    batch = [
        {"Data": json.dumps(row).encode("utf-8"), "PartitionKey": row["chassis_no"]}
        for row in rows
    ]
    # PutRecords can partially fail (e.g. throttling), so retry only the failures
    while batch:
        response = kinesis.put_records(StreamName=args.stream, Records=batch)
        batch = [rec for rec, res in zip(batch, response["Records"]) if "ErrorCode" in res]
        if batch:
            time.sleep(1)
    sent += len(rows)
    print(f"  {sent:,} / {len(df):,}")
    time.sleep(len(rows) / args.rate)

print("Done.")
