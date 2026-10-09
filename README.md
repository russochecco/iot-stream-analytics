# Real-Time IoT Stream Analytics Pipeline
An example of a production-grade, event-driven streaming data pipeline for processing temperature telemetry data, built with PySpark Structured Streaming, Apache Kafka, Apache Iceberg, and InfluxDB. The data flow ingests raw device telemetry, validates payloads against strict schema rules, routes invalid data to a Dead Letter Queue (DLQ), triggers real-time anomaly alerts, and sinks analytical data into both a time-series database and an open lakehouse table, providing analytical capabilities for both online and offline queries.

## Architecture Overview
The system uses a decoupled, event-driven streaming architecture. To ensure fault tolerance, isolation, and scalability, the data flow is divided into two specialized **PySpark Structured Streaming** applications that communicate through **Apache Kafka** topics:

<div align="center">
  <img src="images/blueprint.png" alt="High-Level Blueprint" width="80%">
  <br>
  <p>High-Level Blueprint</p>
</div>

### Component Breakdown

#### 1. Data Ingestion & Source
- **Edge Sensors**: Hardware modules deployed indoors and outdoors stream JSON payloads containing a unique `device_id`, `sensor_id`, `event_timestamp`, `temperature`, and `power` consumption.
- **Raw Topic** (`iot.samples.raw`): Actively ingests the raw, unvalidated JSON streams directly from the edge devices.

#### 2. Application 1: Samples Router
- **Parsing & Cleansing**: Ingests data from the Kafka topic `iot.samples.raw`, parses the JSON against a strict schema contract, and checks for corrupted data or physically impossible anomalies (such as negative power draw).
- **Dynamic Routing**:
	- **Valid Data**: Sent to `iot.samples.refined` for downstream operational processing.
	- **Invalid Data/Dead Letter Queue (DLQ)**: Sent to `iot.samples.dlq` so structurally failing payloads can be isolated and debugged without crashing the main application.
- **Cold Storage Lakehouse**: Simultaneously appends an audit trail of every raw metadata event footprint into the daily partitioned Apache Iceberg table `iot_stream_analytics.events` for offline historical queries.

#### 3. Application 2: Samples Processor
- **Consumption**: Subscribes strictly to the validated data stream coming from `iot.samples.refined`.
- **Metrics & Monitoring**: Computes operational health metrics (like network lag and pipeline latency).
- **SLA & Real-Time Alerting**: Evaluates data against safety thresholds inline (e.g., checks if `temperature` > 85.0°C or `power` > 1500W). If an anomaly occurs, it instantly publishes a notification to `iot.alerts.anomaly`.
- **Hot Storage Sink**: Distributes and streams micro-batches in parallel across worker nodes into the InfluxDB bucket `iot-stream-analytics` for real-time visualization dashboards.

## Physical Deployment & Data Context
The pipeline processes telemetry from a specialized network of multi-sensor hardware modules deployed **inside and outside target rooms**. Each physical asset is tracked via unique `device_id` and `sensor_id` mappings, streaming three core dimensions:
- **Temperature** (`temperature`): Captures the localized ambient/room temperature where the specific sensor ID is physically installed (supporting both indoor climates and sub-zero outdoor environments).
- **Power Consumption** (`power`): Monitors the real-time electrical draw of the localized equipment.
- **Temporal Markers** (`event_timestamp`): Provides the exact event-generation time directly from the edge.


## Tech Stack & Patterns
- **Stream Processing**: PySpark Structured Streaming (`foreachBatch` mechanics).
- **Message Broker**: Apache Kafka (multi-topic orchestration, DLQ pattern, alerting).
- **Storage & Lakehouse**: Apache Iceberg (ACID transactions, daily partitioning).
- **Time Series DB**: InfluxDB v2 (thread-safe worker partition chunking via `influxdb-client`).
- **Resiliency & Observability**: Memory and disk micro-batch persistence (`StorageLevel.MEMORY_AND_DISK`), checkpointing recovery systems, and descriptive application logging.

## Getting Started

### Prerequisites
- Python 3.10+
- Apache Spark 3.5+ (configured with Kafka & Iceberg runtime JARs to match the deploy targets)
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

## Validation & Routing Rules
- **Valid**: Payload structurally conforms to the JSON layout and device `power` is a positive value.
- **Invalid (DLQ Bound)**: Payload contains JSON corruption, structural syntax malformations, missing required fields, or physically impossible anomalies (such as negative power draw).

*Note: Sub-zero temperature readings are treated as valid data to support outdoor environmental sensors. Furthermore, payloads containing critical threshold breaches (e.g., temperature > 85.0°C or power > 1500W) are explicitly handled as structurally valid and passed downstream so they can successfully trigger real-time alerts in the Samples Processor application.*

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
