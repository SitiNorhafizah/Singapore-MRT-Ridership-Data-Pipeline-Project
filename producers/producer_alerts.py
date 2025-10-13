import os
import json
import time
import random
from kafka import KafkaProducer
from dotenv import load_dotenv
from uuid import uuid4
import socket

# Load environment variables
load_dotenv()

# Local JSON file with dummy alerts
LOCAL_JSON = os.getenv("LOCAL_ALERTS_JSON", "./TrainServiceAlerts.json")
TOPIC = os.getenv("ALERTS_TOPIC", "train_alerts")

# Auto-detect Kafka bootstrap server
def detect_bootstrap():
    try:
        # If running inside Docker, "kafka" hostname resolves
        socket.gethostbyname("kafka")
        print("ℹ️ Running inside Docker. Using 'kafka:29092'")
        return "kafka:29092"
    except socket.gaierror:
        # fallback to host localhost
        print("ℹ️ Running on host. Using '127.0.0.1:9092'")
        return "127.0.0.1:9092"

BOOTSTRAP = detect_bootstrap()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

def load_from_file(path):
    """Load local JSON file safely."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print("❌ Failed to load local file:", e)
        return None

def extract_alerts(raw):
    """Flatten nested LTA JSON into list of alert dicts."""
    alerts = []
    value = raw.get("value", {})
    messages = value.get("Message", [])
    for msg in messages:
        alerts.append({
            "alert_id": str(uuid4()),
            "message": msg.get("Content"),
            "timestamp": int(time.time())  # Spark-friendly timestamp
        })
    return alerts

if __name__ == "__main__":
    print(f"🚨 Producer (alerts) starting — Kafka: {BOOTSTRAP}, topic: {TOPIC}")

    while True:
        data = load_from_file(LOCAL_JSON)
        if data is None:
            print("⚠️ No data available — retrying in 30s")
            time.sleep(30)
            continue

        records = extract_alerts(data)

        for rec in records:
            try:
                producer.send(TOPIC, rec)
            except Exception as e:
                print("❌ Send failed for record:", rec, "Error:", e)

        producer.flush()
        print(f"✅ Published {len(records)} records from {LOCAL_JSON} to {TOPIC}")

        # Sleep 30–35 seconds to avoid flooding
        time.sleep(30 + random.randint(0, 5))

