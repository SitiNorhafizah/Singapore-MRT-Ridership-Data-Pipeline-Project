import os
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv
import socket

load_dotenv()

LOCAL_JSON = os.getenv("LOCAL_CROWD_JSON", "./PCDRealTime.json")
TOPIC = os.getenv("CROWD_TOPIC", "station_crowd")

# Auto-detect Kafka bootstrap server
def detect_bootstrap():
    try:
        socket.gethostbyname("kafka")
        return os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    except socket.gaierror:
        return os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

BOOTSTRAP = detect_bootstrap()

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

def load_from_file(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print("❌ Failed to load local file:", e)
        return None

if __name__ == "__main__":
    print(f"🚉 Producer (crowd) starting — Kafka: {BOOTSTRAP}, topic: {TOPIC}")

    while True:
        data = load_from_file(LOCAL_JSON)
        if data is None:
            print("⚠️ No data available — retrying in 30s")
            time.sleep(30)
            continue

        # Ensure records is always a list
        records = data if isinstance(data, list) else [data]

        for rec in records:
            rec["_ingestion_time"] = int(time.time())
            try:
                producer.send(TOPIC, rec)
            except Exception as e:
                print("❌ Send failed:", e)

        producer.flush()
        print(f"✅ Published {len(records)} records from {LOCAL_JSON} to {TOPIC}")

        time.sleep(30)  # adjust interval as needed

