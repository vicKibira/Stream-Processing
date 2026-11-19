#!/usr/bin/env python3
"""
Pizza Sales Kafka Producer
Streams pizza_sales.csv row-by-row to topic: pizza_orders
"""

import csv
import json
import time
import sys
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC         = os.getenv("KAFKA_TOPIC",  "pizza_orders")
CSV_PATH      = os.getenv("CSV_PATH",     "data/pizza_sales.csv")
DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY", "0.1"))  # 100ms between events

def make_producer(retries: int = 10, backoff: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[producer]   Connected to Kafka @ {KAFKA_BROKER}")
            return p
        except NoBrokersAvailable:
            print(f"[producer]   Attempt {attempt}/{retries} — broker not ready, retrying in {backoff}s...")
            time.sleep(backoff)
    print("[producer]   Could not connect to Kafka. Exiting.")
    sys.exit(1)

def build_event(row: dict) -> dict:
    return {
        "pizza_id":          int(float(row["pizza_id"])),
        "order_id":          int(float(row["order_id"])),
        "pizza_name_id":     row["pizza_name_id"],
        "pizza_name":        row["pizza_name"],
        "pizza_category":    row["pizza_category"],
        "pizza_size":        row["pizza_size"],
        "pizza_ingredients": [i.strip() for i in row["pizza_ingredients"].split(",")],
        "quantity":          int(float(row["quantity"])),
        "unit_price":        float(row["unit_price"]),
        "total_price":       float(row["total_price"]),
        "order_date":        row["order_date"],
        "order_time":        row["order_time"],
        "event_ts":          time.time(),
    }

def on_send_success(meta):
    print(f"[producer]    topic={meta.topic}  partition={meta.partition}  offset={meta.offset}")

def on_send_error(exc):
    print(f"[producer]   send error: {exc}")

def main():
    producer = make_producer()
    total = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            event = build_event(row)
            producer.send(
                TOPIC,
                key=event["order_id"],
                value=event,
            ).add_callback(on_send_success).add_errback(on_send_error)

            total += 1
            if total % 100 == 0:
                producer.flush()
                print(f"[producer]   {total} events sent so far...")

            time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"[producer] 🏁  Done. Total events sent: {total}")

if __name__ == "__main__":
    main()
