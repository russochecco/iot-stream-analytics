import time
import json
import random
import logging
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_SAMPLES_RAW_TOPIC = "iot.samples.raw"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
APP_NAME = "IoTSamplesGenerator"

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)s {APP_NAME}: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S"
)

log = logging.getLogger(APP_NAME)


def json_serializer(data):
    """Encodes value dictionaries into standard utf-8 JSON bytes."""
    return json.dumps(data).encode("utf-8")


def string_serializer(key):
    """Encodes string message keys cleanly into standard utf-8 bytes."""
    if key is None:
        return None
    return str(key).encode("utf-8")


def generate_stream():
    """Generates a continuous, realistic live data stream containing edge-case scenarios."""
    producer = None

    # Block and wait for Kafka broker availability
    while not producer:
        try:
            # Using stable function references with active warnings filters active
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=json_serializer,
                key_serializer=string_serializer
            )
        except Exception:
            log.warning("Kafka broker not available yet. Retrying in 3 seconds...")
            time.sleep(3)

    log.info(f"Connected to Kafka. Starting mock samples generation onto topic: '{KAFKA_SAMPLES_RAW_TOPIC}'...")

    devices = [
        {"device_id": "DEV-A100", "sensor_id": "S-TH01", "temperature": 22.0, "power": 450.0},
        {"device_id": "DEV-B200", "sensor_id": "S-TH02", "temperature": 45.0, "power": 800.0},
        {"device_id": "DEV-C300", "sensor_id": "S-TH03", "temperature": 78.0, "power": 1350.0},
    ]

    counter = 0

    try:
        while True:
            counter += 1
            device = random.choice(devices)
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # Scenario A: Corrupt payload (Forces a routing rule failure to Dead Letter Queue)
            if counter % 25 == 0:
                log.info("Simulating payload corruption (DLQ Bound)...")
                producer.send(
                    KAFKA_SAMPLES_RAW_TOPIC,
                    key=device["device_id"],
                    value="{\"device_id\": \"" + device["device_id"] + "\", \"corrupt_json: true"
                )
                time.sleep(0.8)
                continue

            # Scenario B: Impossible Negative Telemetry Value (Forces validation fail -> DLQ)
            elif counter % 40 == 0:
                log.info("Simulating invalid structural metric data (DLQ Bound)...")
                payload = {
                    "device_id": device["device_id"],
                    "sensor_id": device["sensor_id"],
                    "event_timestamp": now_iso,
                    "temperature": -99.0,
                    "power": float(device["power"])
                }

            # Scenario C: Spike Telemetry (Triggers processor stream rule anomaly alert notification)
            elif counter % 15 == 0:
                log.info("Simulating critical equipment metric surge (Processor Alert Bound)...")
                payload = {
                    "device_id": device["device_id"],
                    "sensor_id": device["sensor_id"],
                    "event_timestamp": now_iso,
                    "temperature": round(random.uniform(86.0, 110.0), 2),
                    "power": round(random.uniform(1550.0, 1800.0), 2)
                }

            # Scenario D: Standard operating metrics
            else:
                payload = {
                    "device_id": device["device_id"],
                    "sensor_id": device["sensor_id"],
                    "event_timestamp": now_iso,
                    "temperature": round(device["temperature"] + random.uniform(-2.5, 2.5), 2),
                    "power": round(device["power"] + random.uniform(-30.0, 30.0), 2)
                }

            # Dispatches record to entry broker
            producer.send(KAFKA_SAMPLES_RAW_TOPIC, key=payload["device_id"], value=payload)
            log.info(
                f"Dispatched sample {counter}: device-id={payload['device_id']} temperature={payload.get('temperature')} power={payload.get('power')}")

            time.sleep(0.8)  # Throttle delay rate

    except KeyboardInterrupt:
        log.info("Samples generation terminated by user request.")
    finally:
        if producer:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    generate_stream()
