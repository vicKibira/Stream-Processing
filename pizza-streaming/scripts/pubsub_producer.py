#!/usr/bin/env python3
"""
GCP Pub/Sub Producer
Streams pizza_sales.csv events to Pub/Sub topic (for the GCP pipeline)
Run after:  gcloud auth application-default login
"""

import csv
import json
import os
import time
from google.cloud import pubsub_v1

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TOPIC_ID   = os.getenv("PUBSUB_TOPIC", "pizza-orders")
CSV_PATH   = os.getenv("CSV_PATH",     "data/pizza_sales.csv")
DELAY      = float(os.getenv("PRODUCER_DELAY", "0.05"))

publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def build_event(row: dict) -> dict:
    return {
        "pizza_id":          int(float(row["pizza_id"])),
        "order_id":          int(float(row["order_id"])),
        "pizza_name_id":     row["pizza_name_id"],
        "pizza_name":        row["pizza_name"],
        "pizza_category":    row["pizza_category"],
        "pizza_size":        row["pizza_size"],
        "pizza_ingredients": row["pizza_ingredients"],
        "quantity":          int(float(row["quantity"])),
        "unit_price":        float(row["unit_price"]),
        "total_price":       float(row["total_price"]),
        "order_date":        row["order_date"],
        "order_time":        row["order_time"],
        "event_ts":          time.time(),
    }

total = 0
futures = []

with open(CSV_PATH, newline="", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        event   = build_event(row)
        payload = json.dumps(event).encode("utf-8")
        future  = publisher.publish(topic_path, data=payload, order_id=str(event["order_id"]))
        futures.append(future)
        total += 1
        if total % 200 == 0:
            # flush batch
            for f in futures:
                f.result()
            futures = []
            print(f"[pubsub-producer]   {total} messages published...")
        time.sleep(DELAY)

for f in futures:
    f.result()

print(f"[pubsub-producer]   Done. Total: {total}")
