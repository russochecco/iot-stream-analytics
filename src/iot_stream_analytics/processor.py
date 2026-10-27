import os
import sys
import logging
import pyspark.sql.functions as F
from functools import partial
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from iot_stream_analytics.utils.spark_helpers import setup_logging, get_spark_session, SAMPLE_SCHEMA

APP_NAME = "IoTStreamProcessor"

log = logging.getLogger(APP_NAME)


def compute_telemetry_metrics(parsed_df: DataFrame) -> DataFrame:
    """Flattens incoming payload structures and computes pipeline latency metrics."""
    return (
        parsed_df.select("sample.*", "kafka_timestamp", "ingestion_timestamp")
        .withColumn(
            "network_lag_sec",
            (F.col("kafka_timestamp").cast("long") - F.col("event_timestamp").cast("long")).cast("double")
        )
        .withColumn(
            "ingestion_lag_sec",
            (F.col("ingestion_timestamp").cast("long") - F.col("kafka_timestamp").cast("long")).cast("double")
        )
    )


def extract_out_of_bounds_anomalies(telemetry_df: DataFrame) -> DataFrame:
    """Filters out-of-bounds anomaly telemetry records based on physical safety thresholds."""
    return telemetry_df.filter((F.col("temperature") > 85.0) | (F.col("power") > 1500.0))


def write_influx_points(partition_it, url, token, org, bucket, max_chunk_size):
    """Write an RDD partition iterator of telemetry points to InfluxDB.

    Exceptions here bubble up cleanly through Spark back to the Driver log context.
    """
    with InfluxDBClient(url=url, token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        points = []

        for row in partition_it:
            point = (
                Point("device-telemetry")
                .tag("device_id", row["device_id"])
                .tag("sensor_id", row["sensor_id"])
                .field("temperature", float(row["temperature"]))
                .field("power", float(row["power"]))
                .field("network_lag_sec", float(row["network_lag_sec"]))
                .field("ingestion_lag_sec", float(row["ingestion_lag_sec"]))
                .time(row["event_timestamp"])
            )
            points.append(point)

            if len(points) >= max_chunk_size:
                write_api.write(bucket=bucket, org=org, record=points, write_precision=WritePrecision.NS)
                points = []

        if points:
            write_api.write(bucket=bucket, org=org, record=points, write_precision=WritePrecision.NS)


def process_batch(batch_df: DataFrame, batch_id: int, config: dict):
    """Process, transform, and route a streaming micro-batch."""
    log.info(f"Micro-batch {batch_id} started")

    kafka_bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
    kafka_alerts_anomaly_topic = config["KAFKA_ALERTS_ANOMALY_TOPIC"]
    influx_url = config["INFLUX_URL"]
    influx_token = config["INFLUX_TOKEN"]
    influx_org = config["INFLUX_ORG"]
    influx_bucket = config["INFLUX_BUCKET"]
    max_chunk_size = config["MAX_CHUNK_SIZE"]

    # 1. Parse and extract JSON byte values from Kafka
    samples_df = (
        batch_df.select(
            F.from_json(F.col("value").cast("string"), SAMPLE_SCHEMA).alias("sample"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp")
        )
    )

    # 2. Call Functional Core to compute lag metrics
    telemetry_df = compute_telemetry_metrics(samples_df).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        # 3. Target structural anomaly payloads and route safely
        alerts_df = extract_out_of_bounds_anomalies(telemetry_df)

        # Spark safely skips empty writes instantly—no take(1) check needed
        (
            alerts_df
            .withColumn("value", F.to_json(F.struct(
                "device_id", "sensor_id", "event_timestamp", "temperature", "power"
            )))
            .select(F.col("device_id").alias("key"), "value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("topic", kafka_alerts_anomaly_topic)
            .save()
        )

        # 4. Freeze InfluxDB parameters via partial function mapping
        influx_worker_func = partial(
            write_influx_points,
            url=influx_url,
            token=influx_token,
            org=influx_org,
            bucket=influx_bucket,
            max_chunk_size=max_chunk_size
        )

        # 5. Push data rows horizontally to InfluxDB across cluster executors
        telemetry_df.rdd.foreachPartition(influx_worker_func)
        log.info(f"Micro-batch {batch_id} completed successfully")

    finally:
        # Guaranteed memory/disk drop to protect clustering infrastructure
        telemetry_df.unpersist()


def run_pipeline(spark: SparkSession, config: dict):
    """Build and execute the continuous streaming directed acyclic graph (DAG)."""
    kafka_bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
    starting_offsets = config["KAFKA_STARTING_OFFSETS"]
    kafka_samples_refined_topic = config["KAFKA_SAMPLES_REFINED_TOPIC"]
    spark_checkpoint_location = config["SPARK_CHECKPOINT_LOCATION"]

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("startingOffsets", starting_offsets)
        .option("subscribe", kafka_samples_refined_topic)
        .load()
    )

    process_batch_func = partial(process_batch, config=config)

    query = (
        df.writeStream
        .foreachBatch(process_batch_func)
        .option("checkpointLocation", f"{spark_checkpoint_location}/samples-processor")
        .option("maxOffsetsPerTrigger", 20000)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    setup_logging(APP_NAME)

    pipeline_config = {
        "KAFKA_ALERTS_ANOMALY_TOPIC": os.environ["KAFKA_ALERTS_ANOMALY_TOPIC"],
        "KAFKA_SAMPLES_REFINED_TOPIC": os.environ["KAFKA_SAMPLES_REFINED_TOPIC"],
        "KAFKA_BOOTSTRAP_SERVERS": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "KAFKA_STARTING_OFFSETS": os.environ["KAFKA_STARTING_OFFSETS"],
        "SPARK_CHECKPOINT_LOCATION": os.environ["SPARK_CHECKPOINT_LOCATION"],
        "INFLUX_URL": os.environ["INFLUX_URL"],
        "INFLUX_TOKEN": os.environ["INFLUX_TOKEN"],
        "INFLUX_ORG": os.environ["INFLUX_ORG"],
        "INFLUX_BUCKET": os.environ["INFLUX_BUCKET"],
        "MAX_CHUNK_SIZE": 500
    }

    session = get_spark_session(APP_NAME)

    try:
        run_pipeline(session, pipeline_config)
    except Exception as e:
        log.exception("Streaming processing pipeline terminated unexpectedly:")
        sys.exit(1)
    finally:
        log.info("Stopping Spark Application Session Context safely.")
        session.stop()
