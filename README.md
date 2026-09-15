# Real-Time Fintech Fraud Detection Data Engineering Pipeline

[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-blue)](https://airflow.apache.org/)
[![Redpanda](https://img.shields.io/badge/Streaming-Redpanda-red)](https://redpanda.com/)
[![dbt](https://img.shields.io/badge/Transformation-dbt--core-orange)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/Warehouse-Google%20BigQuery-green)](https://cloud.google.com/bigquery)
[![Looker Studio](https://img.shields.io/badge/BI-Looker%20Studio-yellow)](https://lookerstudio.google.com/)

An end-to-end streaming data pipeline designed to ingest, transform, test, and visualize financial transactions for real-time fraud risk scoring and compliance reporting.

---

## 📊 Live Dashboard
- **Interactive Looker Studio Report:** https://datastudio.google.com/reporting/20949df3-f723-4d70-ac62-6c86e218d5cb

---

## 🏗️ Architecture & Medallion Design

```text
[Transaction Producer] ---> [Redpanda Streaming] ---> [Python Consumer]
                                                            |
                                                            v
[Looker Studio] <--- [dbt Gold/Silver Models] <--- [BigQuery Bronze]
                             ^
                             |
                   [Airflow Orchestrator]
```

### Data Layer Design (Medallion Architecture)
- **Bronze (Raw):** Streamed transactions ingested directly from Redpanda into Google BigQuery as append-only raw JSON logs.
- **Silver (Cleaned & Deduplicated):** Reusable clean models materializing schema constraints, casting data types, and applying window functions (`QUALIFY ROW_NUMBER()`) for streaming deduplication.
- **Gold (Aggregated Business Logic):** Daily aggregation metrics calculating transaction volumes, spending trends, and flagging high-risk actors (`risk_score >= 0.80`) for compliance analysis.

---

## 🛠️ Tech Stack & Engineering Decisions

| Component | Technology | Rationale |
| :--- | :--- | :--- |
| **Streaming Bus** | **Redpanda** | Lightweight, Kafka-compatible event store running via Docker with minimal memory overhead. |
| **Data Warehouse** | **Google BigQuery** | Cloud-native serverless data warehouse supporting native `QUALIFY` SQL extensions. |
| **Transformation** | **dbt (data build tool)** | Version-controlled SQL transformations with 7 automated testing assertions enforcing data quality. |
| **Orchestration** | **Apache Airflow** | Scheduled and dependency-managed DAG orchestration driving dbt models and quality checks. |
| **Visualization** | **Google Looker Studio** | Native BigQuery connectivity for executive fraud risk monitoring. |

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- GCP Service Account Key with BigQuery Admin privileges

### Step-by-Step Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/JahanzaibQasim/fintech-fraud-pipeline
   cd fintech-fraud-pipeline
   ```

2. **Start the Infrastructure Stack:**
   ```bash
   docker compose up -d
   ```

3. **Run Streaming Producer & Consumer:**
   ```bash
   # Terminal 1: Event Producer
   python 01_transaction_producer.py

   # Terminal 2: Consumer Ingest
   python 02_consumer_ingest.py
   ```

4. **Access UI Endpoints:**
   - **Airflow Web UI:** `http://localhost:8081`
   - **Redpanda Console:** `http://localhost:8080`

---

## 🧪 Data Quality & Testing
All models undergo rigorous quality assurance before materializing into Gold layers:
- **`unique` & `not_null` assertions** on transaction IDs and primary keys.
- **Accepted value constraints** on currency codes and risk score boundaries.

To execute tests manually:
```bash
dbt test --project-dir fintech_dbt
```
