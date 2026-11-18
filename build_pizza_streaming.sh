#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="pizza-streaming"

echo "🍕 Scaffolding pizza-streaming project..."

# ─── Directory structure ───────────────────────────────────────────────
mkdir -p $PROJECT_ROOT/{kafka/{producer,consumer},terraform,data,scripts}

# ─── Copy CSV ──────────────────────────────────────────────────────────
cp /mnt/user-data/uploads/pizza_sales.csv $PROJECT_ROOT/data/pizza_sales.csv

# ══════════════════════════════════════════════════════════════════════
# docker-compose.yml  — KRaft mode, no Zookeeper
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/docker-compose.yml << 'EOF'
version: "3.9"

services:
  kafka:
    image: apache/kafka:latest
    container_name: pizza-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: "pizza-cluster-kraft-001"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 10s
      timeout: 10s
      retries: 10

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: pizza-kafka-ui
    ports:
      - "8080:8080"
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_CLUSTERS_0_NAME: pizza-local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
EOF

# ══════════════════════════════════════════════════════════════════════
# requirements.txt
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/requirements.txt << 'EOF'
kafka-python==2.0.2
pandas==2.2.2
EOF

# ══════════════════════════════════════════════════════════════════════
# kafka/producer/producer.py
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/kafka/producer/producer.py << 'EOF'
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
            print(f"[producer] ✅  Connected to Kafka @ {KAFKA_BROKER}")
            return p
        except NoBrokersAvailable:
            print(f"[producer] ⏳  Attempt {attempt}/{retries} — broker not ready, retrying in {backoff}s...")
            time.sleep(backoff)
    print("[producer] ❌  Could not connect to Kafka. Exiting.")
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
    print(f"[producer] ✉️   topic={meta.topic}  partition={meta.partition}  offset={meta.offset}")

def on_send_error(exc):
    print(f"[producer] ❌  send error: {exc}")

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
                print(f"[producer] 📦  {total} events sent so far...")

            time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"[producer] 🏁  Done. Total events sent: {total}")

if __name__ == "__main__":
    main()
EOF

# ══════════════════════════════════════════════════════════════════════
# kafka/consumer/consumer.py
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/kafka/consumer/consumer.py << 'EOF'
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
            print(f"[consumer] ✅  Connected to Kafka @ {KAFKA_BROKER}  |  group={GROUP_ID}")
            return c
        except NoBrokersAvailable:
            print(f"[consumer] ⏳  Attempt {attempt}/{retries} — broker not ready, retrying in {backoff}s...")
            time.sleep(backoff)
    print("[consumer] ❌  Could not connect to Kafka. Exiting.")
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
    print(f"  📊  LIVE SUMMARY  |  orders={stats['total_orders']}  revenue=${stats['total_revenue']:,.2f}")
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
    print(f"[consumer] 👂  Listening on topic: {TOPIC} ...")

    try:
        for msg in consumer:
            event = msg.value
            process(event)

            # log every incoming event
            print(
                f"[consumer] 🍕  order={event.get('order_id')}  "
                f"{event.get('pizza_name')}  "
                f"({event.get('pizza_size')})  "
                f"${event.get('total_price'):.2f}  "
                f"[{event.get('order_date')} {event.get('order_time')}]"
            )

            # print rolling summary every 500 orders
            if stats["total_orders"] % 500 == 0:
                print_summary()

    except KeyboardInterrupt:
        print("\n[consumer] 🛑  Interrupted by user.")
    finally:
        print_summary()
        consumer.close()
        print("[consumer] 🏁  Consumer closed.")

if __name__ == "__main__":
    main()
EOF

# ══════════════════════════════════════════════════════════════════════
# terraform/main.tf
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/terraform/main.tf << 'EOF'
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Enable required APIs ───────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ── GCS bucket for Dataflow staging & temp ────────────────────────
resource "google_storage_bucket" "dataflow_bucket" {
  name          = "${var.project_id}-pizza-dataflow"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 7 }
  }

  depends_on = [google_project_service.apis]
}

# upload CSV to GCS so Dataflow can read it
resource "google_storage_bucket_object" "pizza_csv" {
  name   = "data/pizza_sales.csv"
  bucket = google_storage_bucket.dataflow_bucket.name
  source = "${path.module}/../data/pizza_sales.csv"
}

# ── Pub/Sub topic ─────────────────────────────────────────────────
resource "google_pubsub_topic" "pizza_orders" {
  name = "pizza-orders"

  message_retention_duration = "86600s" # 24 hrs

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "pizza_orders_sub" {
  name  = "pizza-orders-sub"
  topic = google_pubsub_topic.pizza_orders.name

  ack_deadline_seconds       = 20
  retain_acked_messages      = false
  message_retention_duration = "86600s"

  expiration_policy { ttl = "" } # never expires

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# ── BigQuery dataset + table ──────────────────────────────────────
resource "google_bigquery_dataset" "pizza" {
  dataset_id = "pizza_analytics"
  location   = var.region

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_table" "pizza_orders" {
  dataset_id          = google_bigquery_dataset.pizza.dataset_id
  table_id            = "pizza_orders"
  deletion_protection = false

  schema = jsonencode([
    { name = "pizza_id",          type = "INTEGER", mode = "NULLABLE" },
    { name = "order_id",          type = "INTEGER", mode = "NULLABLE" },
    { name = "pizza_name_id",     type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_name",        type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_category",    type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_size",        type = "STRING",  mode = "NULLABLE" },
    { name = "pizza_ingredients", type = "STRING",  mode = "NULLABLE" },
    { name = "quantity",          type = "INTEGER", mode = "NULLABLE" },
    { name = "unit_price",        type = "FLOAT",   mode = "NULLABLE" },
    { name = "total_price",       type = "FLOAT",   mode = "NULLABLE" },
    { name = "order_date",        type = "STRING",  mode = "NULLABLE" },
    { name = "order_time",        type = "STRING",  mode = "NULLABLE" },
    { name = "event_ts",          type = "FLOAT",   mode = "NULLABLE" },
  ])
}

# ── Service account for Dataflow ──────────────────────────────────
resource "google_service_account" "dataflow_sa" {
  account_id   = "pizza-dataflow-sa"
  display_name = "Pizza Dataflow Service Account"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# ── Dataflow job: Pub/Sub → BigQuery ─────────────────────────────
resource "google_dataflow_job" "pubsub_to_bq" {
  name              = "pizza-pubsub-to-bq"
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/PubSub_to_BigQuery"
  temp_gcs_location = "gs://${google_storage_bucket.dataflow_bucket.name}/tmp"

  parameters = {
    inputTopic      = google_pubsub_topic.pizza_orders.id
    outputTableSpec = "${var.project_id}:${google_bigquery_dataset.pizza.dataset_id}.${google_bigquery_table.pizza_orders.table_id}"
  }

  service_account_email = google_service_account.dataflow_sa.email
  region                = var.region
  machine_type          = "n1-standard-2"
  max_workers           = 3

  on_delete = "cancel"

  depends_on = [
    google_pubsub_topic.pizza_orders,
    google_bigquery_table.pizza_orders,
    google_storage_bucket.dataflow_bucket,
    google_project_iam_member.dataflow_worker,
    google_project_iam_member.pubsub_subscriber,
    google_project_iam_member.bigquery_editor,
    google_project_iam_member.storage_admin,
  ]
}
EOF

# ══════════════════════════════════════════════════════════════════════
# terraform/variables.tf
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/terraform/variables.tf << 'EOF'
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}
EOF

# ══════════════════════════════════════════════════════════════════════
# terraform/outputs.tf
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/terraform/outputs.tf << 'EOF'
output "pubsub_topic" {
  value = google_pubsub_topic.pizza_orders.name
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.pizza_orders_sub.name
}

output "bigquery_table" {
  value = "${google_bigquery_dataset.pizza.dataset_id}.${google_bigquery_table.pizza_orders.table_id}"
}

output "gcs_bucket" {
  value = google_storage_bucket.dataflow_bucket.name
}

output "dataflow_sa_email" {
  value = google_service_account.dataflow_sa.email
}
EOF

# ══════════════════════════════════════════════════════════════════════
# terraform/terraform.tfvars.example
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/terraform/terraform.tfvars.example << 'EOF'
project_id = "your-gcp-project-id"
region     = "us-central1"
EOF

# ══════════════════════════════════════════════════════════════════════
# scripts/create_topic.sh  — create Kafka topic inside container
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/scripts/create_topic.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

TOPIC=${1:-pizza_orders}
PARTITIONS=${2:-3}
REPLICATION=${3:-1}

echo "Creating Kafka topic: $TOPIC (partitions=$PARTITIONS, replication=$REPLICATION)..."

docker exec pizza-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

echo "✅ Topic '$TOPIC' ready."
EOF
chmod +x $PROJECT_ROOT/scripts/create_topic.sh

# ══════════════════════════════════════════════════════════════════════
# scripts/pubsub_producer.py  — GCP mode: pushes CSV events to Pub/Sub
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/scripts/pubsub_producer.py << 'EOF'
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
            print(f"[pubsub-producer] 📦  {total} messages published...")
        time.sleep(DELAY)

for f in futures:
    f.result()

print(f"[pubsub-producer] 🏁  Done. Total: {total}")
EOF

# ══════════════════════════════════════════════════════════════════════
# .gitignore
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/.gitignore << 'EOF'
__pycache__/
*.pyc
*.pyo
.env
venv/
.venv/
terraform/.terraform/
terraform/terraform.tfstate
terraform/terraform.tfstate.backup
terraform/terraform.tfvars
*.log
EOF

# ══════════════════════════════════════════════════════════════════════
# README.md
# ══════════════════════════════════════════════════════════════════════
cat > $PROJECT_ROOT/README.md << 'EOF'
# 🍕 Pizza Sales Streaming Pipeline

Kafka (local KRaft) + GCP (Pub/Sub → Dataflow → BigQuery)

```
pizza-streaming/
├── data/
│   └── pizza_sales.csv           # 48,620 order rows
├── kafka/
│   ├── producer/producer.py      # CSV → Kafka topic
│   └── consumer/consumer.py      # Kafka → live analytics
├── scripts/
│   ├── create_topic.sh           # create Kafka topic via Docker exec
│   └── pubsub_producer.py        # CSV → GCP Pub/Sub (GCP mode)
├── terraform/
│   ├── main.tf                   # Pub/Sub, Dataflow, BigQuery, GCS, IAM
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── docker-compose.yml            # Kafka (KRaft) + Kafka UI
└── requirements.txt
```

---

## LOCAL — Kafka KRaft (Docker)

### 1. Prerequisites
- Docker Desktop (or Docker Engine + Compose plugin)
- Python 3.10+

### 2. Install Python deps
```bash
cd pizza-streaming
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 3. Start Kafka (KRaft mode — no Zookeeper)
```bash
docker compose up -d
```

Wait ~15s for Kafka to be healthy, then verify:
```bash
docker compose ps
# pizza-kafka should show "healthy"
```

Kafka UI is live at: http://localhost:8080

### 4. Create the topic
```bash
bash scripts/create_topic.sh pizza_orders 3 1
```

### 5. Run the consumer (in terminal 1)
```bash
source venv/bin/activate
KAFKA_BROKER=localhost:9092 python3 kafka/consumer/consumer.py
```

### 6. Run the producer (in terminal 2)
```bash
source venv/bin/activate
KAFKA_BROKER=localhost:9092 CSV_PATH=data/pizza_sales.csv PRODUCER_DELAY=0.05 \
  python3 kafka/producer/producer.py
```

You'll see events stream in terminal 1, with a rolling summary every 500 orders.

### 7. Tear down
```bash
docker compose down -v
```

---

## GCP — Pub/Sub → Dataflow → BigQuery

### Prerequisites
- `gcloud` CLI authenticated: `gcloud auth application-default login`
- Terraform >= 1.6: https://developer.hashicorp.com/terraform/install
- Python `google-cloud-pubsub`: `pip install google-cloud-pubsub`

### 1. Set your project
```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-central1"
```

### 2. Configure Terraform vars
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars — set project_id
```

### 3. Init & apply
```bash
terraform init
terraform plan
terraform apply -auto-approve
```

This creates:
- GCS bucket (Dataflow staging)
- Pub/Sub topic `pizza-orders` + subscription
- BigQuery dataset `pizza_analytics` + table `pizza_orders`
- Service account with correct IAM roles
- Dataflow streaming job (Pub/Sub → BigQuery via GCP template)

### 4. Stream events to Pub/Sub
```bash
cd ..
GCP_PROJECT_ID=$GCP_PROJECT_ID \
PUBSUB_TOPIC=pizza-orders \
CSV_PATH=data/pizza_sales.csv \
PRODUCER_DELAY=0.05 \
  python3 scripts/pubsub_producer.py
```

### 5. Query results in BigQuery
```bash
bq query --use_legacy_sql=false \
  "SELECT pizza_category, COUNT(*) AS orders, SUM(total_price) AS revenue
   FROM \`$GCP_PROJECT_ID.pizza_analytics.pizza_orders\`
   GROUP BY pizza_category
   ORDER BY revenue DESC"
```

Or use the BigQuery console: https://console.cloud.google.com/bigquery

### 6. Tear down GCP resources
```bash
cd terraform
terraform destroy -auto-approve
```

---

## Env vars reference

| Variable         | Default         | Description                       |
|------------------|-----------------|-----------------------------------|
| KAFKA_BROKER     | localhost:9092  | Kafka bootstrap server            |
| KAFKA_TOPIC      | pizza_orders    | Topic name                        |
| KAFKA_GROUP_ID   | pizza-analytics-group | Consumer group             |
| AUTO_OFFSET      | earliest        | Offset reset policy               |
| CSV_PATH         | data/pizza_sales.csv | Path to CSV                  |
| PRODUCER_DELAY   | 0.1             | Seconds between events (producer) |
| GCP_PROJECT_ID   | (required)      | GCP project ID (Pub/Sub mode)     |
| PUBSUB_TOPIC     | pizza-orders    | Pub/Sub topic ID                  |

---

## Notes
- Kafka runs in **KRaft mode** (Kafka 3.x+) — no Zookeeper required
- Dataflow uses the official GCP template `PubSub_to_BigQuery`
- The Pub/Sub → BigQuery path expects JSON messages with field names matching the BQ schema
- For production: pin the `apache/kafka` image to a specific version tag
EOF

echo ""
echo "✅  Project scaffolded at: ./$PROJECT_ROOT"
echo ""
tree $PROJECT_ROOT 2>/dev/null || find $PROJECT_ROOT -type f | sort

