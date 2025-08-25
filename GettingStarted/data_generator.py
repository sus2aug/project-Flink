import boto3
import json
import random
import time
from datetime import datetime, timezone

# AWS Kinesis configuration
STREAM_NAME = "umarchine-kinesis-stream"
REGION_NAME = "us-east-1"

# Initialize Kinesis client
kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

# Cities
cities = ["Delhi", "Mumbai", "Bangalore", "Kolkata"]

# Counters
counter = 0

while True:
    counter += 3

    # Generate a valid record
    record = {
        "city": random.choice(cities),
        "temperature": round(random.uniform(10.0, 40.0), 2),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    }

    # Every 10 seconds, send a bad record (remove one key)
    if counter % 10 == 0:
        bad_record = record.copy()
        key_to_remove = random.choice(list(bad_record.keys()))
        bad_record.pop(key_to_remove)
        payload = json.dumps(bad_record)
        print(f"Sending BAD record: {payload}")
    else:
        payload = json.dumps(record)
        print(f"Sending valid record: {payload}")

    # Send to Kinesis
    response = kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=payload,
        PartitionKey=record["city"]  # use city as partition key
    )

    # Sleep for 3 seconds
    time.sleep(3)
