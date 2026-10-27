import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, FloatType, TimestampType

# Rigorous schema mapping used to isolate and validate payload fields
SAMPLE_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("temperature", FloatType(), True),
    StructField("power", FloatType(), True)
])


def setup_logging(app_name):
    """Configures centralized logging diagnostics routing to stdout."""

    target_log = logging.getLogger(app_name)

    if not target_log.hasHandlers():
        console_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            fmt='%(asctime)s %(levelname)s %(name)s: %(message)s',
            datefmt="%y/%m/%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)

        target_log.addHandler(console_handler)

    target_log.setLevel(logging.INFO)


def get_spark_session(app_name) -> SparkSession:
    """
    Initializes or retrieves a standard execution context Spark Session
    optimized for clusters with PySpark worker logging active.
    """

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.pyspark.worker.logging.enabled", "true")
        .getOrCreate()
    )
