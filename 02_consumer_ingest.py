import json
import os
import sys
from google.cloud import bigquery
from kafka import KafkaConsumer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_keys.json"

PROJECT_ID = "ecstatic-armor-506312-m7"
DATASET_ID = "fintech_raw_bronze"
TABLE_ID = "bronze_transactions"
FULL_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

print("Connecting to BigQuery client...")
bq_client = bigquery.Client(project=PROJECT_ID)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

consumer = KafkaConsumer(
    "financial_transactions",
    bootstrap_servers=["localhost:19092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bigquery-ingestion-v5",  # Updated group ID to read all available records
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=3000          # Poll timeout
)

print(f"📥 Free-Tier Batch Consumer started! Listening on 'financial_transactions'...\n")

BATCH_SIZE = 20
payload_batch = []

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            print("... Listening for new transactions from Redpanda ...")
            continue

        for topic_partition, messages in records.items():
            for message in messages:
                transaction = message.value
                payload_batch.append(transaction)
                print(f"Captured Event ID: {transaction.get('transaction_id')} | User: {transaction.get('user_id')}")

                if len(payload_batch) >= BATCH_SIZE:
                    load_job = bq_client.load_table_from_json(
                        payload_batch, FULL_TABLE_REF, job_config=job_config
                    )
                    load_job.result()  # Wait for BigQuery load completion
                    
                    print(f"✅ Successfully batch-loaded {len(payload_batch)} records into BigQuery Bronze layer!")
                    payload_batch.clear()
                    sys.stdout.flush()

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()