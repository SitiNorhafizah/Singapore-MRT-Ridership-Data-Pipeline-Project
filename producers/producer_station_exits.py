import os
import csv
import json
import time
import random
from kafka import KafkaProducer
from dotenv import load_dotenv
import socket

load_dotenv()

CSV_FILE = os.getenv("STATION_CSV", "./station_exits.csv")
TOPIC = os.getenv("STATION_TOPIC", "station_exits")

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

if __name__ == "__main__":
    print(f"🚨 Producer (station_exits) starting — Kafka: {BOOTSTRAP}, topic: {TOPIC}")

    while True:
        try:
            with open(CSV_FILE, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                records = [row for row in reader]

            if not records:
                print(f"⚠️ No records found in {CSV_FILE}")
            else:
                for rec in records:
                    rec["_ingestion_time"] = int(time.time())
                    try:
                        producer.send(TOPIC, rec)
                    except Exception as e:
                        print("❌ Send failed for record:", rec, "Error:", e)

                producer.flush()
                print(f"✅ Published {len(records)} records from {CSV_FILE} to {TOPIC}")

        except FileNotFoundError:
            print(f"❌ CSV file not found: {CSV_FILE}")
        except Exception as e:
            print(f"❌ Failed to read CSV: {e}")

        time.sleep(30 + random.randint(0, 5))  # interval with small jitter

