# Real-Time IoT Stream Analytics Pipeline
An example of a production-grade, event-driven streaming data pipeline for processing temperature telemetry data, built with PySpark Structured Streaming, Apache Kafka, Apache Iceberg, and InfluxDB. The data flow ingests raw device telemetry, validates payloads against strict schema rules, routes invalid data to a Dead Letter Queue (DLQ), triggers real-time anomaly alerts, and sinks analytical data into both a time-series database and an open lakehouse table, providing analytical capabilities for both online and offline queries.

## Architecture Overview
To guarantee loose coupling, fault tolerance, and workload isolation, the pipeline splits the data flow into two specialized Spark streaming applications:

<div align="center">
  <img src="images/blueprint.png" alt="High-Level Blueprint" width="75%">
  <p><em>High-Level Blueprint</em></p>
</div>

### 1. Samples Router
- **Ingestion**: Consumes raw payloads from the initial Kafka input topic (`iot.samples.raw`).
- **Parsing**: Parses JSON records and enforces schema structure constraints.
- **Data Cleansing & Validation**: Checks for corrupted JSON or physically impossible telemetry values (e.g., negative temperatures or power ratings).
- **Dynamic Routing**: Diverts corrupted or invalid messages to a Kafka Dead Letter Queue (DLQ) topic (`iot.samples.dlq`) for debugging, while valid data passes to a refined samples topic (`iot.samples.refined`).
- **Audit Trail**: Appends every incoming event's raw metadata footprint into a daily partitioned **Apache Iceberg lakehouse table** (`iot_stream_analytics.events`).

### 2. Samples Processor
- **Consumption**: Consumes validated telemetry streams from the downstream Kafka refined samples topic (`iot.samples.refined`).
- **Metrics Calculation**: Computes metadata observability metrics like **network lag** and **ingestion pipeline latency** in real time.
- **SLA & Anomaly Monitoring**: Evaluates thresholds inline (`temperature` > 85.0°C or `power` > 1500W). If violated, it surfaces immediate alert notifications back out to an asynchronous Kafka alerts queue (`iot.alerts.anomaly`).
- **Time-Series Sink**: Micro-batches are partitioned and distributed across workers to perform high-throughput chunked writes (500 points/batch max) into an **InfluxDB** bucket (`iot-stream-analytics`) for live monitoring dashboards.


## Tech Stack & Patterns
- **Stream Processing**: PySpark Structured Streaming (`foreachBatch` mechanics)
- **Message Broker**: Apache Kafka (multi-topic orchestration, DLQ pattern, alerting)
- **Storage & Lakehouse**: Apache Iceberg (ACID transactions, daily partitioning)
- **Time Series DB**: InfluxDB v2 (thread-safe worker partition chunking via `influxdb-client`)
- **Resiliency & Observability**: Memory and disk micro-batch persistence (`StorageLevel.MEMORY_AND_DISK`), checkpointing recovery systems, and descriptive application logging.

## Getting Started

### Prerequisites
- Python 3.10+
- Apache Spark 3.4+ (configured with Kafka & Iceberg runtime JARs)
- Running instances of Apache Kafka, InfluxDB v2, MinIO, and your target Catalog (e.g., Hive/REST) for Apache Iceberg.

### Environment Configuration
The scripts rely entirely on decoupled environment properties. Create a `.env` file or export the following properties before launching:

```bash
# Spark Settings
export SPARK_CHECKPOINT_LOCATION="/path/to/spark/checkpoints"

# Kafka Settings
export KAFKA_SAMPLES_RAW_TOPIC="iot.samples.raw"
export KAFKA_SAMPLES_REFINED_TOPIC="iot.samples.refined"
export KAFKA_SAMPLES_DLQ_TOPIC="iot.samples.dlq"
export KAFKA_ALERTS_ANOMALY_TOPIC="iot.alerts.anomaly"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_STARTING_OFFSETS="latest"

# InfluxDB Settings
export INFLUX_URL="http://localhost:18086"
export INFLUX_TOKEN="your-super-secret-auth-token"
export INFLUX_ORG="your-organization"
export INFLUX_BUCKET="iot-stream-analytics"

# Apache Iceberg Settings
export ICEBERG_CATALOG_NAMESPACE="iceberg_catalog.bronze.iot_stream_analytics"
```

## Running the Pipeline
To run the pipeline, submit the jobs to your Spark cluster using `spark-submit` following the examples below.

### 1. Start the Input Router & Validation Pipeline
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2 \
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf spark.sql.catalog.iceberg_catalog="org.apache.iceberg.spark.SparkCatalog" \
  --conf spark.sql.catalog.iceberg_catalog.type="hadoop" \
  --conf spark.sql.catalog.iceberg_catalog.warehouse="your-S3-warehouse-location" \
  --conf spark.sql.catalog.iceberg_catalog.s3.path-style-access="true" \
  --conf spark.hadoop.fs.s3a.access.key="your-minio-account" \
  --conf spark.hadoop.fs.s3a.secret.key="your-minio-password" \
  --conf spark.hadoop.fs.s3a.endpoint="your-S3-endpoint" \
  --conf spark.hadoop.fs.s3a.path.style.access="true" \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled="false" \
  --conf spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
  samples_router.py
```

### 2. Start the Analytics & InfluxDB Processor Pipeline
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  samples_processor.py
```


## Data Contracts

### Expected Input Format (SAMPLE_SCHEMA)
Inbound events must match the following JSON contract layout:

```json
{
  "device_id": "DEV-9982",
  "sensor_id": "TEMP-04",
  "event_timestamp": "2026-10-06T12:00:00.000Z",
  "temperature": 23.4,
  "power": 1200.5
}
```

### Validation & Routing Rules
- **Valid**: Payload conforms to schema; `temperature` and `power` are positive values and do not exceed the maximum thresholds (`temperature` ≤ 85.0°C and `power` ≤ 1500W).
- **Invalid (DLQ Bound)**: JSON corruption, missing fields, negative metric entries, or values exceeding safe operational thresholds (`temperature` > 85.0°C or `power` > 1500W).

## Optimization & Resilience Engineering
- **Distributed InfluxDB Writes**: Instead of collecting data to the driver, writes are executed in parallel across Spark worker partitions (`rdd.foreachPartition`) utilizing specialized connection chunking features.
- **Backpressure Management**: Explicitly bounds stream sizing limits via `maxOffsetsPerTrigger=20000` settings to avoid out-of-memory (OOM) errors during traffic spikes.
- **Caching & Lineage Isolation**: Implements micro-batch data caching via `.persist(StorageLevel.MEMORY_AND_DISK)` to avoid evaluating expensive stream transformations multiple times during multi-branch writes.

## Pipeline Testing via Mock Data Generation
The script `test_iot_stream_analytics_e2e.py`, located in the `tests/iot_stream_analytics` Python package, contains a mock data generator designed to test the pipeline with simulated samples.

To execute this end-to-end test script, run the following command:

```bash
poetry run pytest tests/iot_stream_analytics/test_iot_stream_analytics_e2e.py
```
