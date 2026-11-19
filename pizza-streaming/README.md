# Pizza Sales Streaming Pipeline

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
