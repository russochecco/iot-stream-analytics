import os
import logging
import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, FloatType, TimestampType

KAFKA_DLQ_TOPIC = os.environ["KAFKA_DLQ_TOPIC"]
KAFKA_EVENTS_TOPIC = os.environ["KAFKA_EVENTS_TOPIC"]
KAFKA_RECORDS_TOPIC = os.environ["KAFKA_RECORDS_TOPIC"]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_STARTING_OFFSETS = os.environ["KAFKA_STARTING_OFFSETS"]
SPARK_CHECKPOINT_LOCATION = os.environ["SPARK_CHECKPOINT_LOCATION"]
ICEBERG_CATALOG_NAMESPACE = os.environ["ICEBERG_CATALOG_NAMESPACE"]
ICEBERG_EVENTS_TABLE = f"{ICEBERG_CATALOG_NAMESPACE}.events"
APP_NAME = "temperature-telemetry-router"

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


def process_batch(batch_df, batch_id):
    logger.info(f"Processing micro-batch {batch_id} started")

    # ---------------------
    # Parse batch
    # ---------------------
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
        .persist(StorageLevel.MEMORY_AND_DISK))

    try:
        # ---------------------
        # Parse records
        # ---------------------
        records_df = (
            events_df.select(
                "kafka_key",
                "kafka_value",
                F.from_json(F.col("kafka_value"), RECORD_SCHEMA).alias("record")
            ))

        # ---------------------
        # Validate content
        # ---------------------
        classified_df = records_df.withColumn(
            "target_topic",
            F.when(F.col("record").isNull(), F.lit(KAFKA_DLQ_TOPIC))
            .when(
                (F.col("record.temperature").isNull()) | (F.col("record.temperature") < 0) |
                (F.col("record.power").isNull()) | (F.col("record.power") < 0),
                F.lit(KAFKA_DLQ_TOPIC)
            )
            .otherwise(F.lit(KAFKA_RECORDS_TOPIC))
        )

        # ---------------------
        # Hold original value in case validation fails and publish records to Kafka
        # ---------------------
        kafka_df = classified_df.select(
            F.col("kafka_key").alias("key"),
            F.when(F.col("target_topic") == KAFKA_DLQ_TOPIC, F.col("kafka_value"))
            .otherwise(F.to_json("record"))
            .alias("value"),
            F.col("target_topic").alias("topic")
        )

        if not kafka_df.isEmpty():
            (
                kafka_df.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .save()
            )

        # ---------------------
        # Write events to Iceberg table
        # ---------------------
        events_df.writeTo(ICEBERG_EVENTS_TABLE).append()

        logger.info(f"Processing micro-batch {batch_id} completed succesfully")

    except Exception as e:
        logger.error(f"Processing micro-batch {batch_id} failed due to {e}", exc_info=True)
        raise e
    finally:
        events_df.unpersist()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .getOrCreate()
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_EVENTS_TABLE} (
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

    # ---------------------
    # Load data stream
    # ---------------------
    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("subscribe", KAFKA_EVENTS_TOPIC)
        .load()
    )

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            f"{SPARK_CHECKPOINT_LOCATION}/teletry-router"
        )
        .option("maxOffsetsPerTrigger", 20000)
        .start()
    )

    query.awaitTermination()
