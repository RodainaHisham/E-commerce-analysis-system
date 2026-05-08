# 🛒 E-Commerce Data Pipeline & Dataset

A full end-to-end data engineering pipeline built on a synthetic yet realistic e-commerce dataset. The pipeline ingests raw CSV data, streams it through a multi-layer data lakehouse (Bronze → Gold), and loads it into Snowflake — all orchestrated with Apache Airflow.

---

## 📐 Architecture Overview

<img width="1720" height="812" alt="Architecture Diagram drawio (1)" src="https://github.com/user-attachments/assets/6efe4f94-df85-41a2-9456-0a018032eece" />



---

## 📁 Repository Structure

```

├──Simulation.ipynb       #Stream simulation script
├── e.py                  # Extraction script
├── t.py                  # Bronze ingestion + Gold transformation (Spark)
├── l.py                  # Snowflake loader (Spark)
├── airflow_dag.py        # Airflow DAG orchestrating the full pipeline
└── data/
    └── commerce_data/
        ├── users.csv
        ├── products.csv
        ├── orders.csv
        ├── order_items.csv
        ├── reviews.csv
        └── events.csv
```

---

## 🗂️ Dataset


### 📋 Dataset Contents

| File | Rows | Description |
|------|------|-------------|
| `users.csv` | ~10,000 | User profiles, demographics & signup info |
| `products.csv` | ~2,000 | Product catalog with ratings and pricing |
| `orders.csv` | ~20,000 | Order-level transactions |
| `order_items.csv` | ~60,000 | Items purchased per order |
| `reviews.csv` | ~15,000 | Customer-written product reviews |
| `events.csv` | ~80,000 | User event logs: view, cart, wishlist, purchase |

---

### 🧬 Data Dictionary

**Users (`users.csv`)**

| Column | Description |
|--------|-------------|
| `user_id` | Unique user identifier |
| `name` | Full customer name |
| `email` | Synthetic email address |
| `gender` | Male / Female / Other |
| `city` | City of residence |
| `signup_date` | Account creation date |

**Products (`products.csv`)**

| Column | Description |
|--------|-------------|
| `product_id` | Unique product identifier |
| `product_name` | Product title |
| `category` | Electronics, Clothing, Beauty, Home, Sports, etc. |
| `price` | Actual selling price |
| `rating` | Average product rating |

**Orders (`orders.csv`)**

| Column | Description |
|--------|-------------|
| `order_id` | Unique order identifier |
| `user_id` | User who placed the order |
| `order_date` | Timestamp of the order |
| `order_status` | Completed / Cancelled / Returned |
| `total_amount` | Total order value |

**Order Items (`order_items.csv`)**

| Column | Description |
|--------|-------------|
| `order_item_id` | Unique item identifier |
| `order_id` | Associated order |
| `product_id` | Purchased product |
| `quantity` | Quantity purchased |
| `item_price` | Price per unit |

**Reviews (`reviews.csv`)**

| Column | Description |
|--------|-------------|
| `review_id` | Unique review identifier |
| `user_id` | User who submitted the review |
| `product_id` | Reviewed product |
| `rating` | 1–5 star rating |
| `review_text` | Short synthetic review text |
| `review_date` | Submission date |

**Events (`events.csv`)**

| Column | Description |
|--------|-------------|
| `event_id` | Unique event identifier |
| `user_id` | User performing the event |
| `product_id` | Product interacted with |
| `event_type` | `view` / `cart` / `wishlist` / `purchase` |
| `event_timestamp` | Timestamp of the event |

---

## ⚙️ Pipeline Components

Streaming (`Simulation.ipynb`)
 - simulates an E-commerce platform streaming data by sending data in batches with a previously determind sleep time (0.125s in code)

### Task 1 — Extraction (`e.py`)

- Reads all 6 CSV files from the source directory
- Merges and sorts records chronologically with stream-type priority ordering
- Writes JSON batch files to `/raw_ecommerce/stream/` at a configurable rate (`BATCH_SIZE=200`, `SLEEP_TIME=0.125s`)
- Writes static reference tables (users, products) to `/raw_ecommerce/static/`
- Handles `NaN`/`Inf` values to ensure clean JSON output

### Task 2 — Bronze & Gold Transformation (`t.py`)

PySpark job running on YARN that covers two layers:

**Bronze Layer**
- Reads JSON batches from HDFS using a nested schema
- Flattens the `data` struct into top-level columns
- Partitions output by `stream_type` and writes Parquet to HDFS

**Gold Layer — Star Schema**

Dimensions:
- `dim_users` — deduplicated user profiles
- `dim_products` — product catalog enriched with average ratings and review counts
- `dim_date` — full calendar dimension (year, month, quarter, day name, is_weekend)
- `dim_order_status` — distinct order statuses with surrogate keys
- `dim_event_type` — distinct event types with surrogate keys

Facts:
- `fact_orders` — order items joined with orders, products, and status; includes `line_total`, `is_returned`
- `fact_events` — user events joined with event type dimension; includes `funnel_step`

### Task 3 — Archive (`Airflow BashOperator`)

- Copies processed JSON files from the local landing zone to HDFS archive
- Cleans up the local landing zone so the next pipeline run starts fresh

### Task 4 — Snowflake Load (`l.py`)

PySpark job using the Snowflake Spark connector:

- **Dimensions** use an atomic table swap pattern (`_TEMP` → `RENAME` or `SWAP + DROP`) to ensure zero-downtime refreshes
- **Facts** use incremental append with `left_anti` join deduplication on surrogate keys (`order_item_id`, `event_id`)
- Schema changes are handled by dropping and recreating fact tables when column definitions change

---

## Airflow DAG
The pipeline is orchestrated as a linear Airflow DAG:
Copy_to_HDFS → Extracting_data → Transformation → Archive_raw_files_to_HDFS → Loading

| Task | Operator | Description |
|------|----------|-------------|
| Copy_to_HDFS | BashOperator | Creates HDFS directories and uploads raw stream/static JSON files from the Jupyter container |
| Extracting_data | BashOperator | Runs e.py inside the Spark/Jupyter container |
| Transformation | BashOperator | Runs t.py via spark-submit on YARN |
| Archive_raw_files_to_HDFS | BashOperator | Moves JSON to HDFS archive and clears landing zone |
| Loading | BashOperator | Runs l.py with Snowflake connector JARs |

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Orchestration | Apache Airflow |
| Processing | Apache Spark (PySpark) on YARN |
| Storage | HDFS (Hadoop) |
| Data Warehouse | Snowflake |
| Data Format | CSV → JSON (stream) → Parquet (lakehouse) |
| Language | Python 3.11 |

---

##  Getting Started

### Prerequisites

- Docker with `spark-jupyter` and `hadoop-namenode` containers running
- Airflow installed and configured with access to Docker
- Snowflake account with `ECOMMERCE_DB` database and `GOLD_LAYER` schema created

### Running the Pipeline

**Manual execution (step by step):**

```bash
# Step 1 — Stream raw data
docker exec -e HADOOP_USER_NAME=root spark-jupyter \
  spark-submit /home/jovyan/work/e.py

# Step 2 — Bronze + Gold transformation
docker exec -e HADOOP_USER_NAME=root spark-jupyter \
  spark-submit /home/jovyan/work/t.py

# Step 3 — Load to Snowflake
docker exec -e HADOOP_USER_NAME=root spark-jupyter \
  spark-submit \
  --packages net.snowflake:snowflake-jdbc:3.13.33,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3 \
  /home/jovyan/work/l.py
```

**Via Airflow:**

Trigger the `Ecommerce_Pipeline` DAG from the Airflow UI or CLI:

```bash
airflow dags trigger Ecommerce_Pipeline
```



