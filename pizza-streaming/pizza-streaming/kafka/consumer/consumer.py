#!/usr/bin/env python3
"""
Pizza Sales Kafka Consumer
Reads from topic: pizza_orders
Tracks live revenue, top categories, and order counts
"""

import json
import os
import sys
import time
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "localhost:9092")
TOPIC           = os.getenv("KAFKA_TOPIC",     "pizza_orders")
GROUP_ID        = os.getenv("KAFKA_GROUP_ID",  "pizza-analytics-group")
AUTO_OFFSET     = os.getenv("AUTO_OFFSET",     "earliest")

# ── in-memory aggregations ──────────────────────────────────────────
stats = {
    "total_orders":   0,
    "total_revenue":  0.0,
    "by_category":    defaultdict(lambda: {"orders": 0, "revenue": 0.0}),
    "by_size":        defaultdict(lambda: {"orders": 0, "revenue": 0.0}),
    "top_pizzas":     defaultdict(int),
}

def make_consumer(retries: int = 10, backoff: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset=AUTO_OFFSET,
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=30000,
            )
            print(f"[consumer]   Connected to Kafka @ {KAFKA_BROKER}  |  group={GROUP_ID}")
            return c
        except NoBrokersAvailable:
            print(f"[consumer] ⏳  Attempt {attempt}/{retries} — broker not ready, retrying in {backoff}s...")
            time.sleep(backoff)
    print("[consumer]   Could not connect to Kafka. Exiting.")
    sys.exit(1)

def process(event: dict):
    cat  = event.get("pizza_category", "Unknown")
    size = event.get("pizza_size",     "Unknown")
    rev  = event.get("total_price",    0.0)
    name = event.get("pizza_name",     "Unknown")

    stats["total_orders"]  += 1
    stats["total_revenue"] += rev

    stats["by_category"][cat]["orders"]  += 1
    stats["by_category"][cat]["revenue"] += rev

    stats["by_size"][size]["orders"]  += 1
    stats["by_size"][size]["revenue"] += rev

    stats["top_pizzas"][name] += 1

def print_summary():
    print("\n" + "═" * 60)
    print(f"    LIVE SUMMARY  |  orders={stats['total_orders']}  revenue=${stats['total_revenue']:,.2f}")
    print("═" * 60)

    print("\n  By Category:")
    for cat, v in sorted(stats["by_category"].items()):
        print(f"    {cat:<12}  orders={v['orders']:>5}  revenue=${v['revenue']:>10,.2f}")

    print("\n  By Size:")
    for size, v in sorted(stats["by_size"].items()):
        print(f"    {size:<4}  orders={v['orders']:>5}  revenue=${v['revenue']:>10,.2f}")

    print("\n  Top 5 Pizzas:")
    top5 = sorted(stats["top_pizzas"].items(), key=lambda x: x[1], reverse=True)[:5]
    for name, cnt in top5:
        print(f"    {cnt:>4}x  {name}")
    print()

def main():
    consumer = make_consumer()
    print(f"[consumer]  Listening on topic: {TOPIC} ...")

    try:
        for msg in consumer:
            event = msg.value
            process(event)

            # log every incoming event
            print(
                f"[consumer]  order={event.get('order_id')}  "
                f"{event.get('pizza_name')}  "
                f"({event.get('pizza_size')})  "
                f"${event.get('total_price'):.2f}  "
                f"[{event.get('order_date')} {event.get('order_time')}]"
            )

            # print rolling summary every 500 orders
            if stats["total_orders"] % 500 == 0:
                print_summary()

    except KeyboardInterrupt:
        print("\n[consumer] Interrupted by user.")
    finally:
        print_summary()
        consumer.close()
        print("[consumer] 🏁  Consumer closed.")

if __name__ == "__main__":
    main()
