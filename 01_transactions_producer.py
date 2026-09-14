import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=["localhost:19092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "financial_transactions"

CATEGORIES = ["grocery", "electronics", "travel", "luxury_goods", "online_gaming", "wire_transfer"]
COUNTRIES = ["US", "UK", "CA", "AE", "PK", "SG"]
HIGH_RISK_USER = [f"user{i}" for i in range(100,120)]

def generate_transactions():

    is_high_risk_user = random.random() < 0.20

    if is_high_risk_user:
        user_id = random.choice(HIGH_RISK_USER)
        amount = round(random.uniform(500.0, 5000.0), 2)
        risk_score = round(random.uniform(0.80, 0.99), 2)
    else:
        user_id = f"usr_{random.randint(1000,9999)}"
        amount = round(random.uniform(1.0, 300.0), 2)
        risk_score = round(random.uniform(0.01, 0.40), 2)  

    transaction = {
        "transaction_id": f"tx_{fake.uuid4()[:12]}",
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
        "currency": "USD",
        "merchant_category": random.choice(CATEGORIES),
        "device_ip": fake.ipv4(),
        "location_country": random.choice(COUNTRIES),
        "is_card_present": random.choice([True, False]),
        "risk_score": risk_score,
    }

    return transaction

try:
    while True:
        count = 0
        tx = generate_transactions()

        producer.send(TOPIC_NAME, value= tx)

        count +=1
        print(f"[{count}] Emitted: {tx['transaction_id']} | User: {tx['user_id']} | Amount: ${tx['amount']}")
        time.sleep(random.uniform(0.2,0.5))
except KeyboardInterrupt:
    print("\n🛑 Stopping stream.....")

finally:
    producer.flush()
    producer.close()