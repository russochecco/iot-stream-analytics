import os
import sys
import datetime
import logging
import pyspark.sql.functions as F
from functools import partial
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, FloatType, TimestampType
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

KAFKA_ALERTS_TOPIC = os.environ["KAFKA_ALERTS_TOPIC"]
KAFKA_RECORDS_TOPIC = os.environ["KAFKA_RECORDS_TOPIC"]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_STARTING_OFFSETS = os.environ["KAFKA_STARTING_OFFSETS"]
SPARK_CHECKPOINT_LOCATION = os.environ["SPARK_CHECKPOINT_LOCATION"]
INFLUX_URL = os.environ["INFLUX_URL"]
INFLUX_TOKEN = os.environ["INFLUX_TOKEN"]
INFLUX_ORG = os.environ["INFLUX_ORG"]
INFLUX_BUCKET = os.environ["INFLUX_BUCKET"]
APP_NAME = "temperature-telemetry-processor"
MAX_CHUNK_SIZE = 500

RECORD_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("temperature", FloatType(), True),
    StructField("power", FloatType(), True)
])

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s %(levelname)s {APP_NAME}: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S"
)

logger = logging.getLogger(APP_NAME)


def write_influx_points(partition_it, url, token, org, bucket):
    # ---------------------
    # Executes cleanly inside Spark Workers. Configs explicitly passed via partial
    # ---------------------

    with InfluxDBClient(url=url, token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        points = []

        for row in partition_it:
            dt_obj = row["event_timestamp"]

            nanoseconds_timestamp = int(dt_obj.timestamp() * 1e9)

            point = Point("device_telemetry") \
                .tag("device_id", row["device_id"]) \
                .tag("sensor_id", row["sensor_id"]) \
                .field("temperature", float(row["temperature"])) \
                .field("power", float(row["power"])) \
                .field("network_lag_sec", float(row["network_lag_sec"])) \
                .field("ingestion_lag_sec", float(row["ingestion_lag_sec"])) \
                .time(nanoseconds_timestamp)

            points.append(point)

            if len(points) >= MAX_CHUNK_SIZE:
                write_api.write(bucket=bucket, org=org, record=points)

                points = []

        if points:
            write_api.write(bucket=bucket, org=org, record=points)


def process_batch(batch_df, batch_id):
    logger.info(f"Processing micro-batch {batch_id} started")

    # ---------------------
    # Parse batch
    # ---------------------
    record_df = (
        batch_df.select(
            F.from_json(F.col("value").cast("string"), RECORD_SCHEMA).alias("record"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp")
        )
    )

    # ---------------------
    # Telemetry with additional metrics
    # ---------------------
    telemetry_df = (
        record_df.select("record.*", "kafka_timestamp", "ingestion_timestamp")
        .withColumn(
            "network_lag_sec",
            (
                    F.col("kafka_timestamp").cast("long") - F.col("event_timestamp").cast("long")
            )
            .cast("double")
        )
        .withColumn(
            "ingestion_lag_sec",
            (
                    F.col("ingestion_timestamp").cast("long") - F.col("kafka_timestamp").cast("long")
            )
            .cast("double")
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:

        # ---------------------
        # Detect anomalies and publish alerts to Kafka
        # ---------------------
        alerts_df = telemetry_df.filter((F.col("temperature") > 85.0) | (F.col("power") > 1500.0))

        if not alerts_df.isEmpty():
            logger.info(f"Processing micro-batch {batch_id} detected anomalies, sending alerts to Kafka")

            (
                alerts_df
                .withColumn("value", F.to_json(F.struct(
                    "device_id",
                    "sensor_id",
                    "event_timestamp",
                    "temperature",
                    "power"
                )))
                .select(F.col("device_id").alias("key"), "value")
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("topic", KAFKA_ALERTS_TOPIC)
                .save()
            )

        else:
            logger.info(f"Processing micro-batch {batch_id} no anomalies detected")

        # ---------------------
        # Write telemetry data to InfluxDB
        # ---------------------
        influx_worker_func = partial(
            write_influx_points,
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            bucket=INFLUX_BUCKET
        )

        telemetry_df.rdd.foreachPartition(influx_worker_func)

        logger.info(f"Processing micro-batch {batch_id} completed succesfully")

    except Exception as e:
        logger.error(f"Processing micro-batch {batch_id} failed due to {e}", exc_info=True)
        raise e
    finally:
        telemetry_df.unpersist()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )

    # ---------------------
    # Load data stream
    # ---------------------
    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("subscribe", KAFKA_RECORDS_TOPIC)
        .load()
    )

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            f"{SPARK_CHECKPOINT_LOCATION}/telemetry-processor"
        )
        .option("maxOffsetsPerTrigger", 20000)
        .start()
    )

    query.awaitTermination()
