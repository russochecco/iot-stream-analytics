import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Provides a local, isolated in-memory SparkSession for unit testing.
    Insulates the test lifecycle from external standalone clusters.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TelemetryTestingSandbox")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()
