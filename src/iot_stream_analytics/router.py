import os
import sys
import logging
import pyspark.sql.functions as F
from functools import partial
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from iot_stream_analytics.utils.spark_helpers import setup_logging, get_spark_session, SAMPLE_SCHEMA

APP_NAME = "IoTStreamRouter"

log = logging.getLogger(APP_NAME)


def classify_and_route_events(parsed_events_df: DataFrame, dlq_topic: str, refined_topic: str) -> DataFrame:
    """Evaluates data completeness, applies business thresholds, and classifies target output topics."""
    classified = parsed_events_df.withColumn(
        "target_topic",
        F.when(F.col("sample").isNull(), F.lit(dlq_topic))
        .when(
            (F.col("sample.temperature").isNull()) | (F.col("sample.temperature") < 0) |
            (F.col("sample.power").isNull()) | (F.col("sample.power") < 0),
            F.lit(dlq_topic)
        )
        .otherwise(F.lit(refined_topic))
    )

    return classified.select(
        F.col("kafka_key").alias("key"),
        F.when(F.col("target_topic") == dlq_topic, F.col("kafka_value"))
        .otherwise(F.to_json("sample"))
        .alias("value"),
        F.col("target_topic").alias("topic")
    )


def initialize_iceberg_storage(spark: SparkSession, events_table_path: str):
    """Ensure destination structural database tables exist inside the catalog architecture."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {events_table_path} (
            kafka_key STRING,
            kafka_value STRING,
            topic STRING,
            partition INTEGER,
            offset INTEGER,
            kafka_timestamp TIMESTAMP,
            ingestion_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_timestamp))
        """)


def process_batch(batch_df: DataFrame, batch_id: int, config: dict):
    """Classify, validate, split, and route a streaming ingestion micro-batch."""
    log.info(f"Micro-batch {batch_id} started")

    kafka_samples_dlq_topic = config["KAFKA_SAMPLES_DLQ_TOPIC"]
    kafka_samples_refined_topic = config["KAFKA_SAMPLES_REFINED_TOPIC"]
    kafka_bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
    iceberg_events_table = config["ICEBERG_EVENTS_TABLE"]

    # 1. Parse operational metadata records into a common staging DataFrame
    events_df = (
        batch_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("kafka_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp")
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # All exceptions inside the batch cleanly propagate to halt the stream query context safely
    try:
        # 2. Extract nested structural metrics from raw string bodies
        samples_df = events_df.withColumn("sample", F.from_json(F.col("kafka_value"), SAMPLE_SCHEMA))

        # 3. Call Functional Core to apply validation and topic classification rules
        kafka_df = classify_and_route_events(
            parsed_events_df=samples_df,
            dlq_topic=kafka_samples_dlq_topic,
            refined_topic=kafka_samples_refined_topic
        )

        # 4. Stream data downstream safely (Spark safely ignores empty writes—no take(1) needed)
        (
            kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .save()
        )

        # 5. Append structural audit trails directly into Apache Iceberg table targets
        events_df.writeTo(iceberg_events_table).append()

        log.info(f"Micro-batch {batch_id} completed successfully")

    finally:
        # Guaranteed cache cleanup even if structural failures happen mid-batch
        events_df.unpersist()


def run_pipeline(spark: SparkSession, config: dict):
    """Build and start the stream splitting and message routing loop engine."""
    kafka_bootstrap_servers = config["KAFKA_BOOTSTRAP_SERVERS"]
    starting_offsets = config["KAFKA_STARTING_OFFSETS"]
    kafka_samples_raw_topic = config["KAFKA_SAMPLES_RAW_TOPIC"]
    spark_checkpoint_location = config["SPARK_CHECKPOINT_LOCATION"]

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("startingOffsets", starting_offsets)
        .option("subscribe", kafka_samples_raw_topic)
        .load()
    )

    process_batch_func = partial(process_batch, config=config)

    query = (
        df.writeStream
        .foreachBatch(process_batch_func)
        .option("checkpointLocation", f"{spark_checkpoint_location}/samples-router")
        .option("maxOffsetsPerTrigger", 20000)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    setup_logging(APP_NAME)

    iceberg_catalog_namespace = os.environ["ICEBERG_CATALOG_NAMESPACE"]
    pipeline_config = {
        "KAFKA_SAMPLES_DLQ_TOPIC": os.environ["KAFKA_SAMPLES_DLQ_TOPIC"],
        "KAFKA_SAMPLES_RAW_TOPIC": os.environ["KAFKA_SAMPLES_RAW_TOPIC"],
        "KAFKA_SAMPLES_REFINED_TOPIC": os.environ["KAFKA_SAMPLES_REFINED_TOPIC"],
        "KAFKA_BOOTSTRAP_SERVERS": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "KAFKA_STARTING_OFFSETS": os.environ["KAFKA_STARTING_OFFSETS"],
        "SPARK_CHECKPOINT_LOCATION": os.environ["SPARK_CHECKPOINT_LOCATION"],
        "ICEBERG_EVENTS_TABLE": f"{iceberg_catalog_namespace}.events"
    }

    session = get_spark_session(APP_NAME)

    initialize_iceberg_storage(session, pipeline_config["ICEBERG_EVENTS_TABLE"])

    try:
        run_pipeline(session, pipeline_config)
    except Exception as e:
        log.exception("Streaming router execution terminated unexpectedly:")
        sys.exit(1)
    finally:
        session.stop()
