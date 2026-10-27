import os
import sys
import time
import json
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

APP_NAME = "IoTSamplesGenerator"
log = logging.getLogger(APP_NAME)


def setup_logging():
    """Configures systemic pipeline logging diagnostics."""
    if not log.hasHandlers():
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt='%(asctime)s %(levelname)s %(name)s: %(message)s',
            datefmt="%y/%m/%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        log.addHandler(console_handler)
    log.setLevel(logging.INFO)


def json_serializer(data):
    """Encodes value dictionaries into standard utf-8 JSON bytes."""
    # If raw bytes are passed (e.g. for corrupt payloads), return them directly
    if isinstance(data, bytes):
        return data
    return json.dumps(data).encode("utf-8")


def string_serializer(key):
    """Encodes string message keys cleanly into standard utf-8 bytes."""
    if key is None:
        return None
    return str(key).encode("utf-8")


def generate_stream(config: dict):
    """Generates a continuous, realistic live data stream containing edge-case scenarios."""
    kafka_bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
    kafka_samples_raw_topic = config["KAFKA_SAMPLES_RAW_TOPIC"]

    producer = None

    # Block and wait for Kafka broker availability
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=json_serializer,
                key_serializer=string_serializer
            )
        except Exception:
            log.warning("Kafka broker not available yet. Retrying in 3 seconds...")
            time.sleep(3)

    log.info(f"Connected to Kafka. Starting mock samples generation onto topic: '{kafka_samples_raw_topic}'...")

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
                log.info(f"Simulating payload corruption (DLQ Bound) - Sample {counter}...")

                # FIX: We pass raw broken bytes. The updated json_serializer passes it through unchanged,
                # ensuring an un-parseable JSON structure hits Kafka to trigger a real parser exception.
                corrupt_payload = b"{\"device_id\": \"" + device["device_id"].encode(
                    'utf-8') + b"\", \"corrupt_json: true"

                producer.send(kafka_samples_raw_topic, key=device["device_id"], value=corrupt_payload)
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

            # Dispatches verified structural records to entry broker
            producer.send(kafka_samples_raw_topic, key=payload["device_id"], value=payload)
            log.info(
                f"Dispatched sample {counter}: device-id={payload['device_id']} "
                f"temperature={payload['temperature']} power={payload['power']}"
            )

            time.sleep(0.8)

    except KeyboardInterrupt:
        log.info("Samples generation terminated by user request.")
    finally:
        if producer:
            log.info("Flushing and closing Kafka producer resource linkages safely...")
            producer.flush()
            producer.close()


if __name__ == "__main__":
    setup_logging()

    # Consolidate environmental requirements clean fallback profiles
    generator_config = {
        "KAFKA_SAMPLES_RAW_TOPIC": "iot.samples.raw",
        "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    }

    generate_stream(generator_config)
